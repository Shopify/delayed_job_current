class DelayedJobGenerator < Rails::Generator::Base
  def manifest
    record do |m|
      m.migration_template 'migration.rb', 'db/migrate',
        :assigns => { :migration_name => 'CreateDelayedJobs' },
        :migration_file_name => "create_delayed_jobs"
    end
  end

end

