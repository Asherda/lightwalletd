# Disclaimer
This is an alpha build and is currently under active development. Please be advised of the following:

- This code currently is not audited by an external security auditor, use it at your own risk
- The code **has not been subjected to thorough review** by engineers at the Electric Coin Company
- The ZCash version was forked to ceate this VerusHash version
- We **are actively changing** the codebase and adding features where/when needed

🔒 Security Warnings

The Lightwalletd Server is experimental and a work in progress. Use it at your own risk.

---

# Overview

[lightwalletd](https://github.com/asherda/lightwalletd) is a backend service that provides a bandwidth-efficient interface to the VerusCoin blockchain. Currently, lightwalletd supports the Sapling protocol version as its primary concern. The intended purpose of lightwalletd is to support the development of mobile-friendly shielded light wallets.

lightwalletd is a backend service that provides a bandwidth-efficient interface to the VerusCoin blockchain for mobile and other wallets, such as [Zecwallet](https://github.com/adityapk00/zecwallet-lite-lib).

Lightwalletd has not yet undergone audits or been subject to rigorous testing. It lacks some affordances necessary for production-level reliability. We do not recommend using it to handle customer funds at this time (October 2019).

To view status of [CI pipeline](https://gitlab.com/mdr0id/lightwalletd/pipelines)

Code coverage not implemented yet

Documentation for lightwalletd clients (the gRPC interface) is in `docs/rtd/index.html`. The current version of this file corresponds to the two `.proto` files; if you change these files, please regenerate the documentation by running `make doc`, which requires docker to be installed.
# VerusCoin
This fork of lightwalletd uses the VerusCoin block chain. 
## swig
lightwalletd uses swig to access the C++ VerusCoin hash implementations. Simply running make takes care of the swig generation step.

If you want to run the step manually, you can generate the verushash.go and verushash_wrap.cxx files from the verushash/verushash.i and verushash/verushash.cxx via this swig command: 
```
swig -go  -intgosize 64 -c++ -cgo -gccgo -Wall -v parser/verushash/verushash.i
```
## protoc
lightwalletd uses make to handle the protoc step that turns .proto files into .pb.go files for compilation by go. If you want to run the commands manually, change to the walletrpc directory and run them:
```
cd walletrpc/
protoc service.proto --go_out=plugins=grpc:.
protoc compact_formats.proto --go_out=plugins=grpc:.
```
# Building
Simply run make in the lightwalletd directory after cloning it from github
```
git clone git@github.com:Asherda/lightwalletd.git
cd lightwalletd
make
```
AFter generating C++ code using swig and .pb.go code using the protobuf protoc command the C++ and go modules are compiled and linked into the lightwalletd executable in the lightwalletd directory.
## Libraries
THis version of lightwalletd includes the VerusHash C++ source modules in the github archive and compiles and links those in along with any other dependencies as static code, so the result is a single lightwalletd executable that does not require any separate dynamic libraries.
## verusd
lightwalletd uses the rpc interface of verusd, the VerusCoin daemon, to get block information for the ingestor and clients and to take actions based on the frontend API requests. lightwalletd also passes raw transactions from grpcurl clients back to verusd using the rpc interface.

Load verusd - either using the VerusCli or VerusDesktop depending on your preferences - before starting the lightwalletd service.

Once you've got verusd running, check that it has loaded the Verus chain. The verus program in the VerusCoin cli (or the same program in the VerusCoin desktop) is used to request data and take actions using the VerusCoin RPC. A simple request for the current block count makes a good check on the health and status of verusd:
```
./verus getblockcount
``` 
If verusd is not ready yet then you will need to wait until it finishes loading the block chain. If it is not running then get it  running, lightwalletd can only run on old cached information if verusd is not available and doing so requires twqeaking command line options (note needed).

## lightwalletd
Once verusd is runnig properly and responding correctly to verus RPC requests, make worked so you have a new lightwalletd, time to run the it.
```
./lightwalletd --conf-file ~/.komodo/VRSC/VRSC.conf --log-file /logs/server.log --bind-addr 127.0.0.1:18232
```
Production services will need to deal with certs for SSL and DNS and so on.

If you tail -f /logs/server.log you can watch as lightwalletd runs through the backlog of blocks in the VerusCoin chain. This takes 20 or 30 minutes or so.

Assuming it doesn't panic or throw an exception and continues running, your lightwalletd service is serving information over GRPC and requesting information from the verusd RPC.
## Insecurity
While testing locally I don't bother with certs and security for the server. This is a very insecure aproach and is inadvisable except for limited access local testing setups. Before going into production you'll want to get your DNS and certs setup and everything all nicely secured. The following instructions do NOT do that. They instead use things insecurely to allow simpler testing. Be aware these are test-only techniques and should never be used in production.

We end  up using -insecure every time we run grpcurl since our test setup has an autogenerated self signed cert that matches nothing.
## grpcurl
lightwalletd provides GRPC to its clients. This allows for nice "discoverability" features.

Install grpcurl:
```
go get github.com/fullstorydev/grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl
```
This installs the command into the bin sub-folder of wherever your $GOPATH environment variable points. If this directory is already in your $PATH, then you should be good to go. If not, fix that. You want grpcurl on the path.

Now you can check on what the service you built and installed provides over GRPC:
```
grpcurl  -insecure 127.0.0.1:18232  list
cash.z.wallet.sdk.rpc.CompactTxStreamer
grpc.reflection.v1alpha.ServerReflection
```
Focussing on the TX streamer 
```
grpcurl  -insecure 127.0.0.1:18232  list cash.z.wallet.sdk.rpc.CompactTxStreamer
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetAddressTxids
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetBlock
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetBlockRange
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetIdentity
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetLatestBlock
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetLightdInfo
cash.z.wallet.sdk.rpc.CompactTxStreamer.GetTransaction
cash.z.wallet.sdk.rpc.CompactTxStreamer.RecoverIdentity
cash.z.wallet.sdk.rpc.CompactTxStreamer.RegisterIdentity
cash.z.wallet.sdk.rpc.CompactTxStreamer.RegisterNameCommitment
cash.z.wallet.sdk.rpc.CompactTxStreamer.RevokeIdentity
cash.z.wallet.sdk.rpc.CompactTxStreamer.SendTransaction
cash.z.wallet.sdk.rpc.CompactTxStreamer.UpdateIdentity
cash.z.wallet.sdk.rpc.CompactTxStreamer.VerifyMessage
```
Here's how you invoke the GetBlock via the grpcurl tool:
```
grpcurl  -insecure -d '{"height":643508}' 127.0.0.1:18232 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock
{
  "height": "643508",
  "hash": "00uzIAwjtsrkcN3OPDpTE9NojnFtu1EHXlwiovtkdwk=",
  "prevHash": "SRlkgrfIVe8awQ+DRyldoZitNWibgPH+r2ALAAAAAAA=",
  "time": 1566724745,
  "vtx": [
    {
      "index": "1",`
      "hash": "Usc6d0rODsmbX1GILbN8DI3yKmPOAhQVc3LtRgooGvM=",
      "spends": [
        {
          "nf": "azb7Nvf1YXpQswXc+K0vgSvyK1+q/dDjfp7EjZGQitI="
        }
      ],
      "outputs": [
        {
          "cmu": "VEh5XVQd0grEslerDv94SebKmQpxBJKp9bj8IDiflzE=",
          "epk": "1Rd4rAGVZsDN1zd0IHwgUdhQxtmrDRcJ1idw7Mv/+K8=",
          "ciphertext": "oQsDrrqQHOfrSzof1qG81pk1VHy7B700hmQRW0vou8KA5FNjP5X4eUEP7o3ALooByZ2cjg=="
        }
      ]
    }
  ]
}
```

## validating hashes
In the example just above we fetched block 643508 from lightwalletd's frontend. Note the hash value:
```
00uzIAwjtsrkcN3OPDpTE9NojnFtu1EHXlwiovtkdwk=
```
This is the base64 encoded version, in little endian (Intel/AMD style) order. You can convert that to a hex encoded version using several web sites, for example https://base64.guru/converter/decode/hex - simply paste the base64 in, hit the button, and copy the hex result back out. Using it for the hash above I get:
```
d34bb3200c23b6cae470ddce3c3a5313d3688e716dbb51075e5c22a2fb647709
```
Now we can use verus to check that. If using VerusDesktop then under the Debug menu select "show binary folder" and from there launch a shell and get the block info for block 643508:
```
./verus getblock 643508
{
  "hash": "097764fba2225c5e0751bb6d718e68d313533a3ccedd70e4cab6230c20b34bd3",
  "confirmations": 303313,
  "size": 3524,
  "height": 643508,
  "version": 65540,
  "merkleroot": "2d20eeb27922667744961b4406553169205e3900da6bd22dbb20853abee055ef",
  "segid": -2,
<snip - it goes on for a bit>
``` 
Note the hash, 0977... which does not appear to match. Butif you compare back to front you'll see one is the other in opposite byte (every 2 hex chars) order.
It's easier to see if you put them next to each other and separate out the bytes:
```
d3 4b b3 20 0c 23 b6 ca e4 70 dd ce 3c 3a 53 13 d3 68 8e 71 6d bb 51 07 5e 5c 22 a2 fb 64 77 09
09 77 64 fb a2 22 5c 5e 07 51 bb 6d 71 8e 68 d3 13 53 3a 3c ce dd 70 e4 ca b6 23 0c 20 b3 4b d3
```
As expected, the hashes match, when compared properly.
# Local/Developer docker-compose Usage
[docs/docker-compose-setup.md](./docs/docker-compose-setup.md)

# Local/Developer Usage

First, ensure [Go >= 1.11](https://golang.org/dl/#stable) is installed. Once your go environment is setup correctly, you can build/run the below components.

To build the server, run `make`.

This will build the server binary, where you can use the below commands to configure how it runs.

## To run lightwalletd

Assuming you used `make` to build lightwalletd:

```
./lightwalletd --no-tls-very-insecure=true --conf-file /home/.komodo/VRSC/VRSC.conf --zconf-file /home/zcash/.zcash/zcash.conf --log-file /logs/server.log --bind-addr 127.0.0.1:18232
```

# Production Usage

Ensure [Go >= 1.11](https://golang.org/dl/#stable) is installed.

**x509 Certificates**
You will need to supply an x509 certificate that connecting clients will have good reason to trust (hint: do not use a self-signed one, our SDK will reject those unless you distribute them to the client out-of-band). We suggest that you be sure to buy a reputable one from a supplier that uses a modern hashing algorithm (NOT md5 or sha1) and that uses Certificate Transparency (OID 1.3.6.1.4.1.11129.2.4.2 will be present in the certificate).

To check a given certificate's (cert.pem) hashing algorithm:
```
openssl x509 -text -in certificate.crt | grep "Signature Algorithm"
```

To check if a given certificate (cert.pem) contains a Certificate Transparency OID:
```
echo "1.3.6.1.4.1.11129.2.4.2 certTransparency Certificate Transparency" > oid.txt
openssl asn1parse -in cert.pem -oid ./oid.txt | grep 'Certificate Transparency'
```

To use Let's Encrypt to generate a free certificate for your frontend, one method is to:
1) Install certbot
2) Open port 80 to your host
3) Point some forward dns to that host (some.forward.dns.com)
4) Run
```
certbot certonly --standalone --preferred-challenges http -d some.forward.dns.com
```
5) Pass the resulting certificate and key to frontend using the -tls-cert and -tls-key options.

## To run production SERVER

Example using server binary built from Makefile:

```
./lightwalletd --tls-cert cert.pem --tls-key key.pem --conf-file /home/.komodo/VRSC/VRSC.conf --zconf-file /home/zcash/.zcash/zcash.conf --log-file /logs/server.log --bind-addr 127.0.0.1:18232
```

# Pull Requests

We welcome pull requests! We like to keep our Go code neatly formatted in a standard way,
which the standard tool [gofmt](https://golang.org/cmd/gofmt/) can do. Please consider
adding the following to the file `.git/hooks/pre-commit` in your clone:

```
#!/bin/sh

modified_go_files=$(git diff --cached --name-only -- '*.go')
if test "$modified_go_files"
then
    need_formatting=$(gofmt -l $modified_go_files)
    if test "$need_formatting"
    then
        echo files need formatting:
        echo gofmt -w $need_formatting
        exit 1
    fi
fi
```

You'll also need to make this file executable:

```
$ chmod +x .git/hooks/pre-commit
```

Doing this will prevent commits that break the standard formatting. Simply run the
`gofmt` command as indicated and rerun the `git commit` command.
