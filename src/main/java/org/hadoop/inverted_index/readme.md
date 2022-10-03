###基于MapReduce的倒排索引倒排索引
将 FileInputFormat.setInputPaths(job, new Path(args[0]));中文件读入 ，并为所有单词建立
倒排索引，并写入FileOutputFormat.setOutputPath(job, new Path(args[1]));