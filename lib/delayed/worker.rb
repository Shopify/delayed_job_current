module Delayed
  class Worker
    SLEEP = 5
    JOBS_EACH = 100 # Jobs executed in each iteration

    DEFAULT_WORKER_NAME = "host:#{Socket.gethostname} pid:#{Process.pid}" rescue "pid:#{Process.pid}"
    # Indicates that we have catched a signal and we have to exit asap
    cattr_accessor :exit
    self.exit = false

    cattr_accessor :logger
    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    # Every worker has a unique name which by default is the pid of the process (so you only are
    # be able to have one unless override this in the constructor).
    #
    #     Thread.new { Delayed::Worker.new( :name => "Worker 1" ).start }
    #     Thread.new { Delayed::Worker.new( :name => "Worker 2" ).start }
    #
    # There are some advantages to overriding this with something which survives worker retarts:
    # Workers can safely resume working on tasks which are locked by themselves.
    # The worker will assume that it crashed before.
    attr_accessor :name

    # Constraints for this worker, what kind of jobs is gonna execute?
    attr_accessor :min_priority, :max_priority, :job_types

    attr_accessor :quiet

    # A worker will be in a loop trying to execute pending jobs, you can also set
    # a few constraints to customize the worker's behaviour.
    #
    # Named parameters:
    #   - name: the name of the worker, mandatory if you are going to create several workers
    #   - quiet: log to stdout (besides the normal logger)
    #   - min_priority: constraint for selecting what jobs to execute (integer)
    #   - max_priority: constraint for selecting what jobs to execute (integer)
    #   - job_types: constraint for selecting what jobs to execute (String or Array)
    def initialize(options={})
      [ :quiet, :name, :min_priority, :max_priority, :job_types ].each do |attr_name|
        send "#{attr_name}=", options[attr_name]
      end
      # Default values
      self.name  = DEFAULT_WORKER_NAME if self.name.nil?
      self.quiet = true                if self.quiet.nil?
    end

    def start
      say "*** Starting job worker #{name}"

      trap('TERM') { say 'Exiting...'; self.exit = true }
      trap('INT')  { say 'Exiting...'; self.exit = true }

      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = Job.work_off( constraints )
        end

        count = result.sum

        if count.zero?
          sleep(SLEEP) unless self.exit
        else
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end
        break if self.exit
      end

    ensure
      Job.clear_locks! name
    end

    def say(text)
      text = "#{name}: #{text}"
      puts text unless self.quiet
      logger.info text if logger
    end

    protected
      def constraints
        { :max_run_time => Job::MAX_RUN_TIME,
          :worker_name  => name,
          :n            => JOBS_EACH,
          :limit        => 5,
          :min_priority => min_priority,
          :max_priority => max_priority,
          :job_types    => job_types }
      end
  end
end
