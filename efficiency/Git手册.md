#### 没有本地仓库push
```shell script
> echo "# gitpushtest" >> README.md
> git init
> git add README.md
> git commit -m "first commit"
> git branch -M main
> git remote add origin https://github.com/Aaronoooooo/gitpushtest.git
> git push -u origin main
```
#### 已初始化本地库push
```shell script
> git add projectname
> git commit -m "first projectname"
> git branch -M main
> git remote add origin https://github.com/Aaronoooooo/gitpushtest.git
> git branch -M main
> git push -u origin main
```
#### 本地切换dev分支push到git仓库
```shell script
> cd localrepository/
> git checkout -b dev
> vim README.md
> git add README.md
> git commit -m "update"
> git push -v origin dev:dev
```
##### 对于国内的用户,下载 github 上的代码可能比较慢,可以在/etc/hosts 中增加如下配置,可以显著提升 github 的下载速度
```shell script
151.101.72.133 assets-cdn.github.com
151.101.73.194 github.global.ssl.fastly.net
192.30.253.113 github.com
11.238.159.92 git.node5.mirror.et2sqa
```

- git pull冲突问题Commit, stash or revert them to proceed
```shell script
> git stash
> git pull
```
- 查看当前git用户及邮箱
> git config user.name
> git config user.email

- 设置全局用户及邮箱
> git config --global user.name "your name"
> git config --global user.email "your email"

- 设置当前项目用户及邮箱
```shell script
> git config user.name "Aaronoooooo"
> git config user.email "pro1024k@163.com"
```

- 查看git log每次提交所引入的差异
> git log -p -2

- 查看git log graph提交记录
```shell script
> git log --graph --pretty=oneline --abbrev-commit
> git log --graph --oneline
> git log --graph --pretty=format:"%h - %an, %ar : %s"
```

- git合并冲突(master和分支分别有新提交)
```shell script
# 查看冲突文件,删除标记,保留最新文件并保存后在提交
> vim conflict.txt
> git add conflict.txt
> git commit -m "conflict fixed"
# 查看合并记录
> git log --graph --oneline
# 删除分支(可选)
> git branch -d branch_name
```

- gitlab同步fork分支
```shell script
# 在fork的本地项目下添加base(源项目)项目的远程仓库地址
> cd /user/supf/springboot-assembly-mybatis
> git remote add base https://gitee.com/spring/springboot-assembly-mybatis
# 把项目base更新到fork的本地项目里
> git fetch base
# 切换到你自己想要merge的分支
> git checkout master
# merge base项目的更新到你的my工程,把远程fork的base项目的master合并到我的分支
> git merge base/master
```

##### 本地修改Git已Push的Commit信息
```shell script
# 修改最新一次Commit信息
# 方式一(直接commit)
> git commit --amend --no-edit

# 方式二(修改commit message)
> git commit --amend --m="update latest commit" --author="git_account <git_email>"
> git commit --amend --no-edit

# 方式三(重新提交)
> git commit -m 'initial commit'
> git add forgotten_file
> git commit --amend   # 忘记了暂存forgotten_file,最终只会有一个提交——第二次提交将代替第一次提交的结果

# 参数说明
--m : commit message
--author : git账号

# 修改历史Commit信息
> git rebase -i                        (列出所有 Commit 列表)
> git rebase -i HEAD~5                 (列出最近5条 Commit 列表)
> i -> e -> :wq                          (找到要修改的 Commit 记录,将`pick` 修改为 `e`,保存退出)
# git commit --amend --m="update latest" (修正以提交Commit Message)
> git commit --amend                     (vim 模式编辑message)
> git rebase --continue                  (保存并继续修改)

# 参数说明<operation>
--continue : 继续
--skip     : 跳过
--abort    : 中止

# 全局修改邮箱地址
> git filter-branch --commit-filter '
        if [ "$GIT_AUTHOR_EMAIL" = "991696475@qq.com" ];
        then
                GIT_AUTHOR_NAME="Aaronoooooo";
                GIT_AUTHOR_EMAIL="pro1024k@163.com";
                git commit-tree "$@";
        else
                git commit-tree "$@";
        fi' HEAD

# 从每一个commit中移除一个文件
> git filter-branch --tree-filter 'rm -f passwords.txt' HEAD
> git filter-branch --tree-filter --all 'rm -f passwords.txt' HEAD 
# 参数说明
--all : 让 filter-branch 在所有分支上运行

# 查看暂存区文件
> git status
# 移除add暂存区中的文件
> git reset HEAD added.md
# 撤消修改——将它还原成上次提交时的样子或者刚克隆完的样子
> git checkout -- added.md

# 删除被git跟踪的文件,同 .gitignore 类似排除跟踪文件
# 查看被跟踪的文件
> git ls-files
# 移除被跟踪文件
> git rm -r --cached file_name
```
> 查看编辑提交记录  
![1.git_查看提交记录.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbcfo15txj20o004i74n.jpg)  
> 保存退出  
![2.git_修改后保存提示.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbcn97gg1j21ev0770th.jpg)  
> 修正已提交信息  
![3.git_修正已提交信息.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbceev41qj20ql084jsj.jpg)  
> 查看修改结果  
![4.git_查看修改结果.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbceetbarj20n3052wew.jpg)  

##### 删除Git Commit 历史记录
> 方式一:
```shell script
# 新建 reconstruction_branche 分支,并切换
git checkout --orphan reconstruction_branche

# 添加所有本地文件到暂存区
git add .

# 添加提交信息
git commit -m "reconstruction project,delete history commit"

# 强制删除 master 分支
git branch -D master

# 修改当前分支 reconstruction_branche 为 master 分支
git branch -m master

# 强制更新存储库,推送到远程仓库
git push -f origin master
```
> 切换分支并添加到暂存区  
![git提交记录2.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdhz5np4ej20xp0fz0w7.jpg)  
> 替换老分支  
![git提交记录3.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdhz5noksj20x208ymza.jpg)  
> 修改展示  
![git提交记录.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdhz5m4qgj20ri0cb3zz.jpg)  

> 方式二
```shell script
# 删除当前项目中的 .git 文件
rm -rf .git

# 初始化本地库
git init 

# 添加远程仓库
git remote add origin git@github.com:git_user/repository_name

# 添加所有本地文件到暂存区
git add .

# 添加所有本地文件到暂存区
git add .

# 添加提交信息
git commit -m "reconstruction project,delete history commit"

# 强制更新存储库,推送到远程仓库
git push -f origin master
```
>Error1:  
1.OpenSSL SSL_read: Connection was reset, errno 10054  
2.Failed to connect to github.com port 443: Timed out

```shell script
# 解除ssl验证
> git config --global http.sslVerify "false" 
# 检测网络并重试pull/push操作
```


>Error2:提示permission denied问题,使用ssh验证github密钥是否正确
```shell script
> ssh -T git@github.com
Warning: Permanently added the RSA host key for IP address '140.xx.xxx.x' to the list of known hosts.
Hi Aaronoooooo! You've successfully authenticated, but GitHub does not provide shell access.
如果失败,可手动将本地~/.ssh/id_rsa.pub添加到github:Settings -> SSH and GPG keys.
```

>Error3:提示fatal: the remote end hung up unexpectedlyMiB | 1024 bytes/s
```shell script
# 修改全局提交缓存大小
> git config --global http.postBuffer 524288000
 
# 修改全局下载最低速度以及对应的最长时间
> git config --global http.lowSpeedLimit 0
> git config --global http.lowSpeedTime 999999
```

>Error4:push提示error: failed to push some refs t "https://xxxgithub.com",远程库与本地库不一致造成的，在hint中也有提示把远程库同步到本地库就可以了。
```shell script
# 把远程库中的更新合并到(pull=fetch+merge)本地库中，–-rebase的作用是取消掉本地库中刚刚的commit，并把他们接到更新后的版本库之中。
> git pull --rebase origin master
# 忽略历史commit直接合并
> git pull origin master --allow-unrelated-histories()
```
#### IDEA Terminal替换成 Git Bash
> settings–>Tools–>Terminal–>Shell path:"%GIT_HOME%\bin\bash.exe" OR "%GIT_HOME%\bin\sh.exe" --login -i

#### 解决Git Commit注释乱码的问题
```shell script
# 在%GIT_HOME%\etc\bash.bashrc末尾行追加如下内容:
export LANG="zh_CN.UTF-8"
export LC_ALL="zh_CN.UTF-8"
```