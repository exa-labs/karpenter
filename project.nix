{
  makeGoProject,
  fetchNixpkgs,
}:

makeGoProject {
  workspaceRoot = ./.;
  goLock = ./gobuild-nix.lock;
  pkgs = fetchNixpkgs { lockFile = ./project.lock; };
}
