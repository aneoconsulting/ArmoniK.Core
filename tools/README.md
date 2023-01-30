# ArmoniK Core tools to help with things

## Format patch

Formatting with JetBrains Cleanup Code takes a while and the pipeline checks for it too.
When the pipeline fails on formatting, it generates a git patch and stores it as a GitHub pipeline artifact.
You can download the patch, unzip it then apply it with the `git apply patch.diff` command.

We created a [script](./applyformatpatch.sh) to simplify this process.
It is based on the GitHub CLI to download the artifact.
You can find it [here](https://cli.github.com/).

It has to be used a the root of the repository such as:

````bash
sh tools/applyformatpatch.sh
```

Or by making it executable as following:

```bash
chmod +x tools/applyformatpatch.sh
./tools/applyformatpatch.sh
```
