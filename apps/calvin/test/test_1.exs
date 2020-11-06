defmodule CalvinTest do
  use ExUnit.Case
  doctest Calvin
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Example test" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
  after
    Emulation.terminate()
  end
end
