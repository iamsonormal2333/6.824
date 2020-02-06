这个项目是完成mit6.824 2020年春季课程的实验。

2.6更新：完成实验1的所有任务。
实验说明里建议使用sync.Cond即条件变量完成实验。但是我的代码里只用了比较基础的锁来完成。worker构建后会主动向master注册，并向master要求任务。在map任务完成后，mster向worker发送reduce任务。
