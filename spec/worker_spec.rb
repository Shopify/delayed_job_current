require File.dirname(__FILE__) + '/database'

class SimpleJob
  def perform; puts "I'm being run"; end
end

describe Delayed::Worker do
  before do
    Delayed::Job.delete_all
  end
  
  it "should have a default idle_after time of 1 minute" do
    w = Delayed::Worker.new
    w.idle_after.should == 1
  end
  
  it "should have an over-ridable idle_after time" do
    w = Delayed::Worker.new(:idle_after => 2)
    w.idle_after.should == 2
  end
  
  it "should start with next_idle = Time.new + idle_after" do
    worker = Delayed::Worker.new
    worker_thread = Thread.new { worker.start }
    expected_next_idle = Time.new + worker.idle_after.minute
    next_idle = worker.next_idle
    sleep(2)
    Thread.kill(worker_thread)
    next_idle.should be_close(expected_next_idle, 2)
  end
  
  it "should update next_idle after we return from Delayed::Job.work_off" do
    worker = Delayed::Worker.new
    worker_thread = Thread.new { worker.start }
    next_idle_start = worker.next_idle
    sleep(2)
    Delayed::Job.enqueue SimpleJob.new
    sleep(10)
    next_idle_after = worker.next_idle
    Thread.kill(worker_thread)
    next_idle_after.should > next_idle_start
  end
  
  it "should call the on_idle method if Time.new > next_idle" do
    Delayed::Worker.class_eval "def on_idle; @test_var = true; end"
    worker = Delayed::Worker.new
    worker_thread = Thread.new { worker.start }
    worker.instance_variable_defined?(:@test_var).should == false
    sleep(70)
    worker.instance_variable_defined?(:@test_var).should == true
    Thread.kill(worker_thread)
  end
  
end