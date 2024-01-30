module github.com/cosnicolaou/pbzip2/cmd/pbzip2

go 1.21

replace github.com/cosnicolaou/pbzip2 => ../../

require (
	cloudeng.io/cmdutil v0.0.0-20240129012055-33b7697304d1
	cloudeng.io/errors v0.0.9
	github.com/aws/aws-sdk-go v1.50.6
	github.com/cosnicolaou/pbzip2 v1.0.3
	github.com/grailbio/base v0.0.10
	github.com/schollz/progressbar/v2 v2.15.0
	golang.org/x/crypto v0.18.0
)

require (
	cloudeng.io/file v0.0.0-20240129012055-33b7697304d1 // indirect
	cloudeng.io/path v0.0.8 // indirect
	cloudeng.io/text v0.0.11 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/term v0.16.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
