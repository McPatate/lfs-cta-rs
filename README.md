# :hugs: Git LFS Custom transfer agent

Git LFS Custom transfer agent to work with the Hugging Face hub.

The goal is to make file download and upload *blazingly* fast.

## TODO

- [ ] create mspc channel to write progress bytes sent in `upload_chunk`
- [ ] test that `git push` works
- [ ] test with 100mb chunks (update server side code as chunks & urls are determined by the hub)
- [ ] add download
