module github.com/cosnicolaou/pbzip2/cmd/pbzip2

go 1.16

replace github.com/cosnicolaou/pbzip2 => ../../

require (
	cloudeng.io/cmdutil v0.0.0-20210810234338-761e51ad96c2
	cloudeng.io/errors v0.0.6
	github.com/aws/aws-sdk-go v1.40.55
	github.com/cosnicolaou/pbzip2 v0.0.0-00010101000000-000000000000
	github.com/grailbio/base v0.0.10
	github.com/schollz/progressbar/v2 v2.15.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
)

require (
	cloudeng.io/text v0.0.8 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1 // indirect
)
