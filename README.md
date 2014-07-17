# cobar extra rules项目

[cobar](https://github.com/alibaba/cobar)默认提供的路由规则函数无法满足我们数据扩展的需求，所以在参考现有路由functions实现的基础上添加了几个新的路由函数， 纳入当前这个项目单独发布出来。


# 路由函数简介

参看<http://afoo.me/posts/2014-07-17-intro-cobar-extra-rules.html>, 这里不再赘述

# 关于路由规则的配置
参考`conf/`目录下配置文件实例

> 注意：第一条区间规则的左边条件最好设置为业务场景中无法触达的最低边界，比如数字可能直接写0, 日期直接写"0000-00-00 00:00:00"等。


