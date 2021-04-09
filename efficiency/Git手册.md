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

##### 本地修改Git已Push的Commit信息
```shell script
# 修改最新一次Commit信息
> git commit --amend --m="update latest commit" --author="git_account <git_email>"

# 参数说明
--m : commit message
--author : git账号

# 修改历史Commit信息
> `git rebase -i`                        (列出所有 Commit 列表)
> `git rebase -i` HEAD~5                 (列出最近5条 Commit 列表)
> i -> e -> :wq                          (找到要修改的 Commit 记录,将`pick` 修改为 `e`,保存退出)
> git commit --amend --m="update latest" (修正以提交Commit Message)
> git rebase continue                    (保存并继续修改)

# 参数说明<operation>
--continue : 继续
--skip     : 跳过
--abort    : 中止
```
> 查看编辑提交记录
![1.git_查看提交记录.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbcfo15txj20o004i74n.jpg)
> 保存退出
![2.git_修改后保存提示.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbcn97gg1j21ev0770th.jpg)
> 修正已提交信息
![3.git_修正已提交信息.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbceev41qj20ql084jsj.jpg)
> 查看修改结果
![4.git_查看修改结果.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpbceetbarj20n3052wew.jpg)

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
  

