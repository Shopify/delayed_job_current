require File.dirname(__FILE__) + '/database'

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
  
  it "should initialize with next_idle = Time.new + idle_after" do
    w = Delayed::Worker.new
    w.next_idle.should > Time.new + w.idle_after.minute - 2
    w.next_idle.should < Time.new + w.idle_after.minute + 2
  end
  
  it "should update next_idle after we return from Delayed::Job.work_off" do
    
  end
  
  it "should call the on_idle method if Time.new > next_idle" do
    
  end
  
end