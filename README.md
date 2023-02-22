# :hugs: Git LFS Custom transfer agent

Git LFS Custom transfer agent to work with the Hugging Face hub.

The goal is to make file download and upload *blazingly* fast.

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

### Progression bar

Progression bar may show odd numbers at times, you needn't worry about it.

As long as `git push` does not display an error, your file should be downloading/uploading.

## TODO

- [x] create mspc channel to write progress bytes sent in `upload_chunk`
- [x] test that `git push` works
- [x] test with 100mb chunks (update server side code as chunks & urls are determined by the hub)
- [ ] add download
