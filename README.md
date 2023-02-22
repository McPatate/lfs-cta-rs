# :hugs: Git LFS Custom transfer agent

Git LFS Custom transfer agent to work with the Hugging Face hub.

The goal is to make file download and upload *blazingly* fast.

## Known issues

### Progression bar

Progression bar may show odd numbers at times, you needn't worry about it.

As long as `git push` does not display an error, your file should be downloading/uploading.

## TODO

- [x] create mspc channel to write progress bytes sent in `upload_chunk`
- [ ] test that `git push` works
- [ ] test with 100mb chunks (update server side code as chunks & urls are determined by the hub)
- [ ] add download
