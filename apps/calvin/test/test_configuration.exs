defmodule ConfigurationTest do
  use ExUnit.Case
  doctest Configuration

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Configuration partition view works as expected" do
    # create a configuration
    num_replicas = 2
    num_partitions = 3
    configuration = Configuration.new(num_replicas, num_partitions)

    partitions = Configuration.get_partition_view(configuration)
    
    assert length(partitions) == 3, "Expected the partition view to have 3 partitions"
    assert partitions == [1, 2, 3]
  end

  test "Configuration replica view works as expected" do
    # create a configuration
    num_replicas = 3
    num_partitions = 2
    configuration = Configuration.new(num_replicas, num_partitions)

    replicas = Configuration.get_replica_view(configuration)
    
    assert length(replicas) == 3, "Expected the replica view to have 3 replicas"
    assert replicas == [:A, :B, :C]
  end
end
