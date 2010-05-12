require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "andrewtimberlake-delayed_job"
    gem.summary = %Q{Database-backed asynchronous priority queue system -- Extracted from Shopify}
    gem.description = %Q{Delayed_job (or DJ) encapsulates the common pattern of asynchronously executing longer tasks in the background. It is a direct extraction from Shopify where the job table is responsible for a multitude of core tasks.}
    gem.email = "andrew@andrewtimberlake.com"
    gem.homepage = "http://github.com/andrewtimberlake/delayed_job"
    gem.authors = ["Tobias Lütke", "Andrew Timberlake"]
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end
