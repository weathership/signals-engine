{ pkgs, ... }:

{
  # GitHub CLI for tags and releases. Python stays on the repo's uv pin.
  packages = with pkgs; [
    gh
    git
    just
  ];
}
