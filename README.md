# :hugs: Git LFS Custom transfer agent

Git LFS Custom transfer agent to work with the Hugging Face hub.

The goal is to make file download and upload *blazingly* fast.

This custom agent will improve your uploads if your commits contain only a few files. For single file uploads you should see 4 to 6x upload speed improvements.

When uploading a big amount of files, disk read speed and network bandwidth will be maxed out, given you set the git config `lfs.concurrenttransfers` to a big enough value (so no improvements, but upload speed won't be degraded).

## Installation

For now, you have to compile this yourself.

Then run:

```sh
cd /my/git/dir

git config lfs.customtransfer.multipart.path /path/to/binary

# you can edit this value to something that suits your repository
# 8 seems like a sane default to me, as there will be many
# chunk uploads in parallel
git config lfs.customtransfer.multipart.concurrenttransfers 8
```

## Known issues

### Progress bar

Progress bar information is blatantly incorrect, but you needn't worry about it.

As long as `git push` does not display an error, your file should be downloading/uploading.

## TODO

- [x] create mspc channel to write progress bytes sent in `upload_chunk`
- [x] test that `git push` works
- [x] test with 100mb chunks (update server side code as chunks & urls are determined by the hub)
- [ ] add download
