class Puppet::Provider::Mongodb < Puppet::Provider

  # Without initvars commands won't work.
  initvars
  commands :mongo => 'mongo'

  # Mongo Version checker
  def self.mongo_version
    @@mongo_version ||= self.mongo_command('db.version()', {'json' => false}).strip
  end

  def mongo_version
    self.class.mongo_version
  end

  def self.mongo_24?
    v = self.mongo_version
    ! v[/^2\.4\./].nil?
  end

  def mongo_24?
    self.class.mongo_24?
  end

  def self.auth_enabled?
    mongo_command('db.serverCmdLineOpts().parsed.security.authorization', {'json' => false}).strip == "enabled"
  end

  def auth_enabled?
    self.class.auth_enabled?
  end

  def mongo_command(command, args = {})
    self.class.mongo_command(command, args)
  end

  def self.mongo_command(command, args_hash = {})

    # Default to requesting output as json
    unless args_hash['json'] == false
      command = "printjson(#{command})"
    end

    # Prepend rc file execution to allow for authenticatiom
    unless args_hash['rc'] == false
      command = "#{mongorc_command}; #{command}"
    end

    # Allow waiting for mongod to become ready
    # Wait for 2 seconds initially and double the delay at each retry
    wait = 2
    begin
      args = Array.new
      args << '--quiet'

      # Default to running commands in the admin database
      args << (args_hash['db'] || 'admin')
      args << ['--host', args_hash['host']] if args_hash['host']
      args << ['--eval', command]
      output = mongo(args.flatten)
    rescue Puppet::ExecutionFailure => e
      if wait <= 2**(args_hash['retries'] || 0)
        if e.to_s =~ /Error: couldn't connect to server/
          info("Waiting #{wait} seconds for mongod to become available")
        elsif e.to_s =~ /slaveOk=false/
          info("Waiting #{wait} seconds for slaves to sync")
        else
          raise
        end
        sleep wait
        wait *= 2
        retry
      end
    end

    if (args_hash['json'] == false)
      output
    else
      parse_as_json(output)
    end
  end

  # Optional defaults file
  # Used if it exists and mongo_command not passed 'rc' => false
  def self.mongorc_command
    if File.file?("#{Facter.value(:root_home)}/.mongorc.js")
      "load('#{Facter.value(:root_home)}/.mongorc.js')"
    else
      ""
    end
  end

  def self.parse_as_json(str = "")

    # Avoid non-json empty sets
    str = "{}" if str == "null\n"
   warning("NILL!!!!!!!!") if ! str
    # Remove JavaScript objects
    str.gsub!(/ObjectId\(([^)]*)\)/, '\1')
    str.gsub!(/ISODate\((.+?)\)/, '\1 ')
    str.gsub!(/Timestamp\((.+?)\)/, '[\1]')

    JSON.parse(str)
  end
end
