#
# Author: François Charlier <francois.charlier@enovance.com>
#
require File.expand_path(File.join(File.dirname(__FILE__), '..', 'mongodb'))
Puppet::Type.type(:mongodb_replset).provide(:mongodb, :parent => Puppet::Provider::Mongodb) do

  desc "Manage hosts members for a replicaset."

  confine :true =>
    begin
      require 'json'
      true
    rescue LoadError
      false
    end

  commands :mongo => 'mongo'

  mk_resource_methods

  def initialize(resource={})
    super(resource)
    @property_flush = {}
  end

  def members=(hosts)
    @property_flush[:members] = hosts
  end

  def self.instances
    instance = get_replset_properties
    if instance
      # There can only be one replset per node
      [new(instance)]
    else
      []
    end
  end

  def self.prefetch(resources)
    instances.each do |prov|
      if resource = resources[prov.name]
        resource.provider = prov
      end
    end
  end

  def exists?
    @property_hash[:ensure] == :present
  end

  def create
    @property_flush[:ensure] = :present
    @property_flush[:members] = resource.should(:members)
  end

  def destroy
    @property_flush[:ensure] = :absent
  end

  def flush
    set_members
    @property_hash = self.class.get_replset_properties
  end

  private

  def db_ismaster(host)
    mongo_command('db.isMaster().primary', {'host' => host})
  end

  def rs_initiate(conf, master)
    # TODO: use or get rid of master param
    return mongo_command("rs.initiate(#{conf})")
  end

  def rs_status(args = {})
    mongo_command("rs.status()", args)
  end

  def rs_add(host, master)
    # TODO: use or get rid of master param
    mongo_command("rs.add(\"#{host}\")")
  end

  def rs_remove(host, master)
    mongo_command("rs.remove(\"#{host}\")", {'host' => master})
  end

  def master_host(hosts)
    hosts.each do |host|
      # TODO: refactor
      primary = db_ismaster(host)
      if primary
        return primary
      end
    end
    false
  end

  def self.get_replset_properties
    output = mongo_command('rs.conf()', {'retries' => 4})
    if output['members']
      members = output['members'].collect do |val|
        val['host']
      end
      props = {
        :name     => output['_id'],
        :ensure   => :present,
        :members  => members,
        :provider => :mongo,
      }
    else
      props = nil
    end
    Puppet.debug("MongoDB replset properties: #{props.inspect}")
    props
  end

  def alive_members(hosts)
    hosts.select do |host|
      begin
        Puppet.debug "Checking replicaset member #{host} ..."
        status = rs_status({'host' => host})
        if status.has_key?('errmsg') and status['errmsg'] == 'not running with --replSet'
          raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is not supposed to be part of a replicaset."
        end
        if status.has_key?('set')
          if status['set'] != self.name
            raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is already part of another replicaset."
          end

          # This node is alive and supposed to be a member of our set
          Puppet.debug "Host #{host} is available for replset #{status['set']}"
          true
        elsif status.has_key?('info')
          Puppet.debug "Host #{host} is alive but unconfigured: #{status['info']}"
          true
        end
      rescue Puppet::ExecutionFailure
        Puppet.warning "Can't connect to replicaset member #{host}."

        false
      end
    end
  end

  def set_members
    if @property_flush[:ensure] == :absent
      # TODO: I don't know how to remove a node from a replset; unimplemented
      #Puppet.debug "Removing all members from replset #{self.name}"
      #@property_hash[:members].collect do |member|
      #  rs_remove(member, master_host(@property_hash[:members]))
      #end
      return
    end

    if ! @property_flush[:members].empty?
      if auth_enabled?
        # It's likely this is the first run, and only local connections are available due to the
        # admin user not being created yet.  There may be a better check, but authentication
        # failures are difficult to catch.  If the admin user hasn't been created, then the 
        # rs_status call in alive_members will only work correctly when a host is specified if
        # rc => false is also given.  For now, assume that the correct list has been given.
        alive_hosts = @property_flush[:members]
        Puppet.debug "Authentication enabled, assuming all members alive: #{alive_hosts.inspect}"
      else
        # Find the alive members so we don't try to add dead members to the replset
        alive_hosts = alive_members(@property_flush[:members])
        dead_hosts  = @property_flush[:members] - alive_hosts
        raise Puppet::Error, "Can't connect to any member of replicaset #{self.name}." if alive_hosts.empty?
        Puppet.debug "Alive members: #{alive_hosts.inspect}"
        Puppet.debug "Dead members: #{dead_hosts.inspect}" unless dead_hosts.empty?
      end
    else
      alive_hosts = []
    end

    if @property_flush[:ensure] == :present and @property_hash[:ensure] != :present
      Puppet.debug "Initializing the replset #{self.name}"

      # Create a replset configuration
      hostconf = alive_hosts.each_with_index.map do |host,id|
        "{ _id: #{id}, host: \"#{host}\" }"
      end.join(',')
      conf = "{ _id: \"#{self.name}\", members: [ #{hostconf} ] }"

      # Set replset members with the first host as the master
      output = rs_initiate(conf, alive_hosts[0])
      if output['ok'] == 0
        raise Puppet::Error, "rs.initiate() failed for replicaset #{self.name}: #{output['errmsg']}"
      end
    else
      # Add members to an existing replset
      if master = master_host(alive_hosts)
        current_hosts = db_ismaster(master)
        newhosts = alive_hosts - current_hosts
        newhosts.each do |host|
          output = rs_add(host, master)
          if output['ok'] == 0
            raise Puppet::Error, "rs.add() failed to add host to replicaset #{self.name}: #{output['errmsg']}"
          end
        end
      else
        raise Puppet::Error, "Can't find master host for replicaset #{self.name}."
      end
    end
  end

end
