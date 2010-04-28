require File.dirname(__FILE__) + '/database'

class SimpleJob
  def perform; puts "I'm being run"; end
end

describe Delayed::Worker do
  before do
    Delayed::Job.delete_all
  end
  
  describe "#idle_after" do
    it "should default to 1 minute" do
      w = Delayed::Worker.new
      w.idle_after.should == 1.minute
    end
  
    it "should be over-ridable" do
      w = Delayed::Worker.new(:idle_after => 2.minutes)
      w.idle_after.should == 2.minutes
    end
  end
  
  describe "#next_idle" do
    it "should equal Time.now + idle_after" do
      worker = Delayed::Worker.new
      worker_thread = Thread.new { worker.start }
      expected_next_idle = Time.new + worker.idle_after
      next_idle = worker.next_idle
      sleep(2)
      Thread.kill(worker_thread)
      next_idle.should be_close(expected_next_idle, 2)
    end
  
    it "should update after we return from a job" do
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
  end
  
  describe "#on_idle" do
    it "should be called when Time.now > next_idle" do
      Delayed::Worker.class_eval "def on_idle; @test_var = true; end"
      worker = Delayed::Worker.new(:idle_after => 5)
      worker_thread = Thread.new { worker.start }
      worker.instance_variable_defined?(:@test_var).should == false
      sleep(10)
      worker.instance_variable_defined?(:@test_var).should == true
      Thread.kill(worker_thread)
    end
  end
  
end