module Delayed
  class Worker
    SLEEP = 5

    attr_reader :idle_after, :next_idle
    cattr_accessor :logger
    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    def initialize(options={})
      @quiet = options[:quiet]
      @idle_after = options[:idle_after] || 60
      Delayed::Job.min_priority = options[:min_priority] if options.has_key?(:min_priority)
      Delayed::Job.max_priority = options[:max_priority] if options.has_key?(:max_priority)
    end

    def start
      say "*** Starting job worker #{Delayed::Job.worker_name}"
      
      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }
      
      @next_idle = Time.new + @idle_after
      
      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = Delayed::Job.work_off
        end

        count = result.sum

        break if $exit

        if count.zero?
          sleep(SLEEP)
        else
          @next_idle = Time.new + @idle_after
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end
        
        if Time.new > @next_idle
          @next_idle = Time.new + @idle_after
          on_idle
        end
        
        break if $exit
      end

    ensure
      Delayed::Job.clear_locks!
    end

    def say(text)
      puts text unless @quiet
      logger.info text if logger
    end
    
    private
    
    def on_idle
      # You may overide this callback method
    end

  end
end
