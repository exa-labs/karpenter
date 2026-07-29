{
  makeGoProject,
  fetchNixpkgs,
}:

let
  pkgs = fetchNixpkgs { lockFile = ./project.lock; };
  lintSkipped = pkgs.runCommandLocal "karpenter-lint-skipped" { } "touch $out";
in
makeGoProject {
  workspaceRoot = ./.;
  goLock = ./gobuild-nix.lock;
  inherit pkgs;

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

  # The fork's own presubmit owns linting. Its upstream golangci-lint
  # configuration references kubeapilinter, which is not available in the
  # monorepo's pinned linter plugin set.
  passthru.lint = lintSkipped;
}
