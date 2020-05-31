with import <nixpkgs> {};

runCommand "dummy" {
	buildInputs = [
		gcc
		pkg-config
		sqlite
	];
} ""
