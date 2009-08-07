require 'timeout'

module Delayed

  class DeserializationError < StandardError
  end

  # A job object that is persisted to the database.
  # Contains the work object as a YAML field.
  class Job < ActiveRecord::Base
    MAX_ATTEMPTS = 25
    MAX_RUN_TIME = 4.hours
    set_table_name :delayed_jobs

    # By default failed jobs are destroyed after too many attempts.
    # If you want to keep them around (perhaps to inspect the reason
    # for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = true

    # By default successful jobs are destroyed after finished.
    # If you want to keep them around (for statistics/monitoring),
    # set this to false.
    cattr_accessor :destroy_successful_jobs
    self.destroy_successful_jobs = false

    NextTaskSQL         = '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL AND finished_at IS NULL'
    NextTaskOrder       = 'priority DESC, run_at ASC'

    ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

    named_scope :unfinished, :conditions => { :finished_at => nil }
    named_scope :finished,   :conditions => [ "finished_at IS NOT NULL" ]

    class << self
      # When a worker is exiting, make sure we don't have any locked jobs.
      def clear_locks!( worker_name )
        update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
      end

      # Add a job to the queue
      def enqueue(*args, &block)
        object = block_given? ? EvaledJob.new(&block) : args.shift

        unless object.respond_to?(:perform) || block_given?
          raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
        end

        priority = args.first || 0
        run_at   = args[1]

        Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
      end

      # Find a few candidate jobs to run (in case some immediately get locked by others).
      # Return in random order prevent everyone trying to do same head job at once.
      def find_available( options = {} )
        limit        = options[:limit]        || 5
        max_run_time = options[:max_run_time] || MAX_RUN_TIME
        worker_name  = options[:worker_name]  || Worker::DEFAULT_WORKER_NAME

        sql        = NextTaskSQL.dup
        time_now   = db_time_now
        conditions = [time_now, time_now - max_run_time, worker_name]
        if options[:min_priority]
          sql << ' AND (priority >= ?)'
          conditions << options[:min_priority]
        end

        if options[:max_priority]
          sql << ' AND (priority <= ?)'
          conditions << options[:max_priority]
        end

        if options[:job_types]
          sql << ' AND (job_type IN (?))'
          conditions << options[:job_types]
        end
        conditions.unshift(sql)

        ActiveRecord::Base.silence do
          find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
        end
      end

      # Run the next job we can get an exclusive lock on.
      # If no jobs are left we return nil
      def reserve_and_run_one_job( options = {} )
        max_run_time = options[:max_run_time] || MAX_RUN_TIME
        worker_name  = options[:worker_name]  || Worker::DEFAULT_WORKER_NAME
        # For the next jobs availables, try to get lock. In case we cannot get exclusive
        # access to a job we try the next.
        # This leads to a more even distribution of jobs across the worker processes.
        find_available( options ).each do |job|
          t = job.run_with_lock( max_run_time, worker_name )
          return t unless t.nil?  # return if we did work (good or bad)
        end
        # we didn't do any work, all 5 were not lockable
        nil
      end

      # Do num jobs and return stats on success/failure.
      # Exit early if interrupted.
      def work_off( options = {} )
        n = options[:n] || 100
        success, failure = 0, 0

        n.times do
          case reserve_and_run_one_job( options )
            when true
              success += 1
            when false
              failure += 1
            else
              break  # leave if no work could be done
          end
          break if Worker.exit # leave if we're exiting
        end

        return [success, failure]
      end

    end # class << self

    def failed?
      failed_at
    end
    alias_method :failed, :failed?

    def payload_object
      @payload_object ||= deserialize(self['handler'])
    end

    def name
      @name ||= begin
        payload = payload_object
        if payload.respond_to?(:display_name)
          payload.display_name
        else
          payload.class.name
        end
      end
    end

    def payload_object=(object)
      self['job_type'] = object.class.to_s
      self['handler']  = object.to_yaml
    end

    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(message, backtrace = [], time = nil)
      if self.attempts < (payload_object.send(:max_attempts) rescue MAX_ATTEMPTS)
        time ||= Job.db_time_now + (attempts ** 4) + 5

        self.attempts    += 1
        self.run_at       = time
        self.last_error   = message + "\n" + backtrace.join("\n")
        self.unlock
        save!
      else
        Delayed::Worker.logger.info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consequetive failures."
        destroy_failed_jobs ? destroy : update_attribute(:failed_at, Time.now)
      end
    end

    # Try to run one job.
    # Returns true/false (work done/work failed) or nil if job can't be locked.
    def run_with_lock(max_run_time = MAX_RUN_TIME, worker_name = Worker::DEFAULT_WORKER_NAME)
      Delayed::Worker.logger.info "* [JOB] aquiring lock on #{name}"
      unless lock_exclusively!(max_run_time, worker_name)
        # We did not get the lock, some other worker process must have
        Delayed::Worker.logger.warn "* [JOB] failed to aquire exclusive lock for #{name}"
        return nil # no work done
      end

      begin
        runtime =  Benchmark.realtime do
          Timeout.timeout(max_run_time.to_i) { invoke_job }
        end
        destroy_successful_jobs ? destroy :
          update_attribute(:finished_at, Time.now)
        Delayed::Worker.logger.info "* [JOB] #{name} completed after %.4f" % runtime
        return true  # did work
      rescue Exception => e
        reschedule e.message, e.backtrace
        log_exception(e)
        return false  # work failed
      end
    end

    # Lock this job for the worker given as parameter (the name).
    # Returns true if we have the lock, false otherwise.
    def lock_exclusively!(max_run_time, worker)
      now = self.class.db_time_now
      affected_rows = if locked_by != worker
        # We don't own this job so we will update the locked_by name and the locked_at
        self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?)", id, (now - max_run_time.to_i)])
      else
        # We already own this job, this may happen if the job queue crashes.
        # Simply resume and update the locked_at
        self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
      end
      if affected_rows == 1
        self.locked_at    = now
        self.locked_by    = worker
        return true
      else
        return false
      end
    end

    # Unlock this job (note: not saved to DB)
    def unlock
      self.locked_at    = nil
      self.locked_by    = nil
    end

    # This is a good hook if you need to report job processing errors in additional or different ways
    def log_exception(error)
      Delayed::Worker.logger.error "* [JOB] #{name} failed with #{error.class.name}: #{error.message} - #{attempts} failed attempts"
      Delayed::Worker.logger.error(error)
    end

    # Moved into its own method so that new_relic can trace it.
    def invoke_job
      payload_object.perform
    end

  private

    def deserialize(source)
      handler = YAML.load(source) rescue nil

      unless handler.respond_to?(:perform)
        if handler.nil? && source =~ ParseObjectFromYaml
          handler_class = $1
        end
        attempt_to_load(handler_class || handler.class)
        handler = YAML.load(source)
      end

      return handler if handler.respond_to?(:perform)

      raise DeserializationError,
        'Job failed to load: Unknown handler. Try to manually require the appropiate file.'
    rescue TypeError, LoadError, NameError => e
      raise DeserializationError,
        "Job failed to load: #{e.message}. Try to manually require the required file."
    end

    # Constantize the object so that ActiveSupport can attempt
    # its auto loading magic. Will raise LoadError if not successful.
    def attempt_to_load(klass)
       klass.constantize
    end

    # Get the current time (GMT or local depending on DB)
    # Note: This does not ping the DB to get the time, so all your clients
    # must have syncronized clocks.
    def self.db_time_now
      (ActiveRecord::Base.default_timezone == :utc) ? Time.now.utc : Time.now
    end

  protected

    def before_save
      self.run_at ||= self.class.db_time_now
    end

  end

  class EvaledJob
    def initialize
      @job = yield
    end

    def perform
      eval(@job)
    end
  end
end
