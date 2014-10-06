#
# Author: François Charlier <francois.charlier@enovance.com>
#

Puppet::Type.newtype(:mongodb_replset) do
  @doc = "Manage a MongoDB replicaSet"

  ensurable do
    defaultto :present

    newvalue(:present) do
      provider.create
    end
  end

  newparam(:name) do
    desc "The name of the replicaSet"
  end

  newproperty(:members, :array_matching => :all) do
    desc "The replicaSet members"

    def insync?(is)
      is.sort == should.sort
    end
  end

  newproperty(:admin_username) do
    desc "The admin username"

    def insync?(is)
      is.downcase == should.downcase
    end
  end

  newproperty(:admin_password) do
    desc "The admin password"
  end

  autorequire(:package) do
    'mongodb_client'
  end

  autorequire(:service) do
    'mongodb'
  end

  autorequire(:mongodb_conn_validator) do
    'mongodb'
  end
end
