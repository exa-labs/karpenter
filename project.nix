{
  makeGoProject,
  fetchNixpkgs,
}:

makeGoProject {
  workspaceRoot = ./.;
  goLock = ./gobuild-nix.lock;
  pkgs = fetchNixpkgs { lockFile = ./project.lock; };

  # The upstream suite relies on envtest's live control plane, which cannot
  # run in the Nix sandbox. The fork's presubmit owns that suite; keep only
  # hermetic tests in the monorepo's library-project check.
  unitTestSpec = {
    groups = [
      { packages = [ "./pkg/cloudprovider/..." ]; }
      { packages = [ "./pkg/events/..." ]; }
      { packages = [ "./pkg/operator/options/..." ]; }
      { packages = [ "./pkg/utils/atomic/..." ]; }
      { packages = [ "./pkg/utils/controller/..." ]; }
      { packages = [ "./pkg/utils/daemonset/..." ]; }
      { packages = [ "./pkg/utils/pod/..." ]; }
      { packages = [ "./pkg/utils/pretty/..." ]; }
      { packages = [ "./pkg/utils/resources/..." ]; }
      { packages = [ "./pkg/utils/ringbuffer/..." ]; }
    ];
  };
}
