module github.com/cosnicolaou/pbzip2/cmd/pbzip2

go 1.16

replace github.com/cosnicolaou/pbzip2 => ../../

require (
	cloudeng.io/cmdutil v0.0.0-20230427034012-5eaade00fd0f
	cloudeng.io/errors v0.0.8
	cloudeng.io/file v0.0.0-20230427034012-5eaade00fd0f // indirect
	github.com/aws/aws-sdk-go v1.44.255
	github.com/cosnicolaou/pbzip2 v1.0.2
	github.com/grailbio/base v0.0.10
	github.com/schollz/progressbar/v2 v2.15.0
	golang.org/x/crypto v0.8.0
)
