>
>Step 1, download stable version from [file_flow](https://github.com/braveHUI/file_flow.git), untar package

```Bash
git clone https://github.com/braveHUI/file_flow.git
```

>Step 2, conda create file_flow

```Bash
conda create -n file_flow python=3
source activate file_flow
pip install -r requirement.txt
```
>Step 3, Build an image in the docker configuration environment

```Bash
 docker build -t file_flow:v2 .
```

>Step 4, Create a data volume

```Bash
 docker volume create filesname
```
>Step 5, Build the container to run successfully, and mount a host directory as the volume source that represents the host directory to mount, and target represents the location of the volume in the container

```Bash
 docker run -it --mount type=bind,source=/mnt/win/hegh/file/5.14,target=/code/file_flow/testfile/ -v  filesname:/code/file_flow/ file_flow:v7
```