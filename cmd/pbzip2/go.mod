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
