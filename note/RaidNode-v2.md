### RaidNode-v2

#### 功能改进

#### 1.raid粒度提升到目录

- 概要

  - 目录下的所有文件只生成一个校验文件
  - 目录过大时会将目录分为几部分，然后每一部分单独去做raid，最后执行完的map会将其合并到一个文件中
  - 在生成校验块时将每一组原始块到对应校验块的映射都会记录下来（记录blockID），一般是myslq数据库中
  - 为了防止数据损坏，做raid时会记录每个块的crc校验值用来重建时的正确性验证
  - 目录下的文件增删后需要重新做raid,来恢复校验文件

- 实现

  - 配置

    ```java
    /**
    每一种编码解码方式会生成一个codec对象，此对象包含了做raid的条纹长度、校验长度、编解码方式、存储校验文件的目录、临时存放校验文件等信息
    **/  
    
    private Codec(JSONObject json) throws JSONException {
        this.jsonStr = json.toString();
        this.id = json.getString("id");
        this.parityLength = json.getInt("parity_length");
        this.stripeLength = json.getInt("stripe_length");
        this.erasureCodeClass = json.getString("erasure_code");
        this.parityDirectory = json.getString("parity_dir");
        this.priority = json.getInt("priority");
        this.description = getJSONString(json, "description", "");
        this.isDirRaid = Boolean.parseBoolean(getJSONString(json, "dir_raid", "false"));
        this.tmpParityDirectory = getJSONString(
            json, "tmp_parity_dir", "/tmp" + this.parityDirectory);
        this.tmpHarDirectory = getJSONString(
            json, "tmp_har_dir", "/tmp" + this.parityDirectory + "_har");
        this.simulateBlockFix = json.getBoolean("simulate_block_fix"); 
        checkDirectory(parityDirectory);
        checkDirectory(tmpParityDirectory);
        checkDirectory(tmpHarDirectory);
        setPathSeparatorDirectories();
      }
    ```

  - TraggerMonter触发raid操作

    ![traggerMonter](D:\Thinker313\hadoop-20\note\TraggerMonter.jpg)

    - 是否需要重新加载配置文件由文件的最后修改时间与配置的重新加载配置间隔共同决定
    - 是否触发筛选raid文件操作由当前策略任务数与触发间隔一起决定
    - 改进：支持从文件系统中读取策略做raid的目录与文件
    - 从文件系统中读取待riad文件或者目录时检查策略：
      - 如果策略是目录riad
        - 选取文件中配置的目录（如果由文件会跳过）
        - 判断目录中文件包含块的个数是否达到最小raid块个数要求
        - 判断目录创建时间，副本数以及校验文件是否存在等
      - 如果策略时文件raid
        - 判断创建时间，副本数以及校验文件是否存在等
        - 判断块个数等

  - 目录的文件的筛选

    1. 目录筛选还是在线程Processor中进行，Processor之选则符合条件的文件，对于目录raid的筛选有所不同是由LeafDirectoryProcessor进行筛选，它只筛选页目录(目录下全是文件的目录)。

       ```java
       public static DirectoryTraversal raidLeafDirectoryRetriever(
             final PolicyInfo info, List<Path> roots, Collection<PolicyInfo> allInfos,
             final Configuration conf, int numThreads, boolean doShuffle,
             boolean allowStandby)
             throws IOException {
           final RaidState.Checker checker = new RaidState.Checker(allInfos, conf);//筛选条件checker
           final FileSystem fs = FileSystem.get(conf);
           Filter filter = new Filter() {
             @Override
             public boolean check(FileStatus f) throws IOException {
               long now = RaidNode.now();
               if (!f.isDir()) {
                 return false;
               }
               List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(conf,
                   fs, f.getPath());//获取目录下所有文件
               RaidState state = checker.check(info, f, now, false, lfs);//检测符合条件文件
               if (LOG.isDebugEnabled()) {
                 LOG.debug(f.getPath() + " : " + state);
               }
               return state == RaidState.NOT_RAIDED_BUT_SHOULD;
             }
           };
           return new DirectoryTraversal("Raid File Retriever ", roots, fs, filter,
             numThreads, doShuffle, allowStandby, true);
         }
       
       /**
       Processor#run（）
       */
       public void run() {
             this.cache = PlacementMonitor.locatedFileStatusCache.get();
             List<Path> subDirs = new ArrayList<Path>();
             List<FileStatus> filtered = new ArrayList<FileStatus>();
             try {
               while (!finished && totalDirectories.get() > 0) {//finished当traggerMonter取数据中断时被设置为真
                 Path dir = null;
                 try {
                   dir = directories.poll(1000L, TimeUnit.MILLISECONDS);//取出非页目录
                 } catch (InterruptedException e) {
                   continue;
                 }
                 if (dir == null) {
                   continue;
                 }
                 try {
                   filterDirectory(dir, subDirs, filtered);//筛选页目录及子目录
               } catch (Throwable ex) {
                   LOG.error(getName() + " throws Throwable. Skip " + dir, ex);
                 totalDirectories.decrementAndGet();
                   continue;
                 }
                 int numOfDirectoriesChanged = -1 + subDirs.size();
                 if (totalDirectories.addAndGet(numOfDirectoriesChanged) == 0) {
                   interruptProcessors();
                 }
                 submitOutputs(filtered, subDirs);//将筛选出的页目录存放到集合BlockingQueue<FileStatus> output集合中，将扫面出的的子目录存放到BlockingDeque<Path> directories集合中
               }
             } finally {
               // clear the cache to avoid memory leak
               cache.clear();
               PlacementMonitor.locatedFileStatusCache.remove();
               int active = activeThreads.decrementAndGet();
               if (active == 0) {//最后一个线程put
                 while (true) {
                   try {
                     output.put(FINISH_TOKEN);
                     break;
                   } catch (InterruptedException e) {
                   }
                 }
               }
             }
       ```

    2. 具体筛选逻辑

       ```java
        protected void filterDirectory(Path dir, List<Path> subDirs,
               List<FileStatus> filtered) throws IOException {
             subDirs.clear();
             filtered.clear();
             if (dir == null) {
               return;
           }
             FileStatus[] elements;
           if (avatarFs != null) {
               elements = avatarFs.listStatus(dir, true);
             } else {
               elements = fs.listStatus(dir);
             }
             cache.clear();
             if (elements != null) {
               boolean isLeafDir = true;
               for (FileStatus element : elements) {
                 if (element.isDir()) {//目录下所有都是文件才叫页目录
                   subDirs.add(element.getPath());
                   isLeafDir = false;
                 }
               }
               if (isLeafDir && elements.length > 0) {
                 FileStatus dirStat = avatarFs != null?
                     avatarFs.getFileStatus(dir): 
                     fs.getFileStatus(dir);
                 if (filter.check(dirStat)) {//检测目录的状态
                   filtered.add(dirStat);
                 }
               }
             }
           }
       /**
       check
       **/
       public RaidState check(PolicyInfo info, FileStatus file, long now,
               boolean skipParityCheck, List<FileStatus> lfs) throws IOException {
             ExpandedPolicy matched = null;
             long mtime = -1;
             String uriPath = file.getPath().toUri().getPath();
             if (inferMTimeFromName) {
               mtime = mtimeFromName(uriPath);
             }
             // If we can't infer the mtime from the name, use the mtime from filesystem.
             // If the the file is newer than a day, use the mtime from filesystem.
             if (mtime == -1 ||
                 Math.abs(file.getModificationTime() - now) < ONE_DAY_MSEC) {
               mtime = file.getModificationTime();//修改时间检测
             }
             boolean hasNotRaidedButShouldPolicy = false; 
             for (ExpandedPolicy policy : sortedExpendedPolicy) {
               if (policy.parentPolicy == info) {
                 matched = policy;
                 break;
               }
               RaidState rs = policy.match(file, mtime, now, conf, lfs);
               if (rs == RaidState.RAIDED) {
                 return NOT_RAIDED_OTHER_POLICY;
               } else if (rs == RaidState.NOT_RAIDED_BUT_SHOULD) {
                 hasNotRaidedButShouldPolicy = true; 
               } 
             }
             if (matched == null) {
               return NOT_RAIDED_NO_POLICY;
             }
       
             // The preceding checks are more restrictive,
             // check for excluded just before parity check.
             if (shouldExclude(uriPath)) {//可配置非riad目录
               return NOT_RAIDED_NO_POLICY;
             }
             
             if (file.isDir() != matched.codec.isDirRaid) {
               return NOT_RAIDED_NO_POLICY;
             }
       
             long blockNum = matched.codec.isDirRaid?//目录下文件共有的块要大于最小raid块数
                 DirectoryStripeReader.getBlockNum(lfs):
                 computeNumBlocks(file);
       
             if (blockNum <= TOO_SMALL_NOT_RAID_NUM_BLOCKS) {
               return NOT_RAIDED_TOO_SMALL;
             }
             
             RaidState finalState = matched.getBasicState(file, mtime, now,
                 skipParityCheck, conf, lfs);//判断目录下文件块的副本数是否与生成校验文件副本数一致，如果一致判断校验文件是否存在，如果存在就不提交，否则提交raid任务。
             if (finalState == RaidState.RAIDED) {
               return finalState;
             } else if (hasNotRaidedButShouldPolicy) {
               return RaidState.NOT_RAIDED_OTHER_POLICY;
             } else {
               return finalState;
             }
           }
       
       ```

       ![](D:\Thinker313\hadoop-20\note\筛选.jpg)

    3. 目录筛选完毕，对目录进行编码单元的划分

       ```java
        static public List<EncodingCandidate> splitPaths(Configuration conf,
             Codec codec, List<FileStatus> paths) throws IOException {// paths是筛选出来的页目录集合
           List<EncodingCandidate> lec = new ArrayList<EncodingCandidate>();
           long encodingUnit = conf.getLong(RAID_ENCODING_STRIPES_KEY, 
               DEFAULT_RAID_ENCODING_STRIPES);//配置策略由hdfs.raid.stripe.encoding确定，条纹包含的块数
           FileSystem srcFs = FileSystem.get(conf);
           for (FileStatus s : paths) {//遍历path
             if (codec.isDirRaid != s.isDir()) {
               continue;
             }
             long numBlocks = 0L;
             if (codec.isDirRaid) {
               List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(//获取当前目录下的所有文件，为了确保目录为页目录，此方法中还会进行判断，如果包含目录那么方法返回null
                   conf, srcFs, s.getPath());
               if (lfs == null) {
                 continue;
               }
               for (FileStatus stat : lfs) {
                 numBlocks += RaidNode.numBlocks(stat);//统计块总数
               }
             } else {
               numBlocks = RaidNode.numBlocks(s);
             }
             long numStripes = RaidNode.numStripes(numBlocks, codec.stripeLength);//计算当前目录的条纹数，stripeLength为条纹包含块个数
             String encodingId = System.currentTimeMillis() + "." + rand.nextLong();
             for (long startStripe = 0; startStripe < numStripes;
                  startStripe += encodingUnit) { //切分当前遍历目录为多个编码单元
               lec.add(new EncodingCandidate(s, startStripe, encodingId, encodingUnit,
                   s.getModificationTime()));
             }
           }
           return lec;
         }
       ```

       切分目录为多个编码单元可以参考逻辑：假如/home/yarn/data目录包含20个块，条纹长度为7，编码编号encodingId=System.currentTimeMillis() + "." + rand.nextLong()编码单元encodingUnit=2划分单元内容如下

       | 条纹开始编号startStripe | 编码编号      | 编码单元encodingUnit | 目录名          | map任务编码的块区间（startStripe\*codec.stripeLength，（startStripe+encodingUnit）\*codec.stripeLength）-1) |
       | ----------------------- | ------------- | -------------------- | --------------- | ------------------------------------------------------------ |
       | 0                       | ${encodingId} | 1                    | /home/yarn/data | （0,6)                                                       |
       | 1                       | ${encodingId} | 1                    | /home/yarn/data | (7,13)                                                       |
       | 2                       | ${encodingId} | 1                    | /home/yarn/data | (14,,20)                                                     |

       目录/home/yarn/data会划分为三个编码单元，一般情况下每个编码单元对应一个maptask。

    4. 筛选完毕后，配置任务、提交任务

       ```java
       /**
       1. 创建任务配置
       **/
       private static JobConf createJobConf(Configuration conf) {
           JobConf jobconf = new JobConf(conf, DistRaid.class);
           jobName = NAME + " " + dateForm.format(new Date(RaidNode.now()));
           jobconf.setUser(RaidNode.JOBUSER);//任务执行的ugi
           jobconf.setJobName(jobName);
           jobconf.setMapSpeculativeExecution(false);
         RaidUtils.parseAndSetOptions(jobconf, SCHEDULER_OPTION_LABEL);
       
         jobconf.setJarByClass(DistRaid.class);
           jobconf.setInputFormat(DistRaidInputFormat.class);//map输入格式为DistRaidInputFormat
           jobconf.setOutputKeyClass(Text.class);
           jobconf.setOutputValueClass(Text.class);
       
           jobconf.setMapperClass(DistRaidMapper.class);//map为DistRaidMapper
           jobconf.setNumReduceTasks(0);
           return jobconf;
         }
       
       /**
       2. 建立计算切片的输入文件
       */
       private boolean setup() throws IOException {
           estimateSavings();
       
           final String randomId = getRandomId();
           JobClient jClient = new JobClient(jobconf);
           Path jobdir = new Path(jClient.getSystemDir(), NAME + "_" + randomId);
       
           LOG.info(JOB_DIR_LABEL + "=" + jobdir);
           jobconf.set(JOB_DIR_LABEL, jobdir.toString());
           Path log = new Path(jobdir, "_logs");
       
           // The control file should have small size blocks. This helps
           // in spreading out the load from mappers that will be spawned.
           jobconf.setInt("dfs.blocks.size",  OP_LIST_BLOCK_SIZE);
       
           FileOutputFormat.setOutputPath(jobconf, log);
           LOG.info("log=" + log);
       
           // create operation list
           FileSystem fs = jobdir.getFileSystem(jobconf);
           Path opList = new Path(jobdir, "_" + OP_LIST_LABEL);
           jobconf.set(OP_LIST_LABEL, opList.toString());
           int opCount = 0, synCount = 0;
           SequenceFile.Writer opWriter = null;
       
           try {
             opWriter = SequenceFile.createWriter(fs, jobconf, opList, Text.class,
                 PolicyInfo.class, SequenceFile.CompressionType.NONE);//在hdfs上创建计算切片的输入文件
             for (RaidPolicyPathPair p : raidPolicyPathPairList) {//计算出来的编码单元
               // If a large set of files are Raided for the first time, files
               // in the same directory that tend to have the same size will end up
               // with the same map. This shuffle mixes things up, allowing a better
               // mix of files.避免数据倾斜，将大小不同的文件尽可能的打散，为啥这种方式可以打散呢？？
               java.util.Collections.shuffle(p.srcPaths);
               for (EncodingCandidate ec : p.srcPaths) {
               opWriter.append(new Text(ec.toString()), p.policy);
                 opCount++;
               if (++synCount > SYNC_FILE_MAX) {
                   opWriter.sync();
                 synCount = 0;
                 }
               }
             }
     //计算切片输入文件的格式
       //startStripe +" " + encodingId + " " + encodingUnit +" "+ modificationTime + " "+ this.srcStat.getPath().toString() +
     // p.policy
           } finally {
           if (opWriter != null) {
               opWriter.close();
           }
             fs.setReplication(opList, OP_LIST_REPLICATION); // increase replication for control file
         }
           raidPolicyPathPairList.clear();
           
           jobconf.setInt(OP_COUNT_LABEL, opCount);//(OP_COUNT_LABE与NumMapTasks个数设置为一致，都为划分条纹单元的个数
           LOG.info("Number of files=" + opCount);
           jobconf.setNumMapTasks(getMapCount(opCount));
           LOG.info("jobName= " + jobName + " numMapTasks=" + jobconf.getNumMapTasks());
           return opCount != 0;
         } 
       ```

    5. 分片计算

       ```java
       /**
       istRaidInputFormat#getSplits（）
       map数由mapred.map.tasks配置决定，默认是1
       分片的计算是每个分片包含的文件数应该不大于文件总数除以分片数的商
       */
       public InputSplit[] getSplits(JobConf job, int numSplits)//split个数与map数一致，map数与EncodingCandidate个数一致
               throws IOException {
             final int srcCount = job.getInt(OP_COUNT_LABEL, -1);//目录数
             final int targetcount = srcCount / numSplits; //targetcount应是1
             String srclist = job.get(OP_LIST_LABEL, "");
             if (srcCount < 0 || "".equals(srclist)) {
               throw new RuntimeException("Invalid metadata: #files(" + srcCount
                   + ") listuri(" + srclist + ")");
             }
             Path srcs = new Path(srclist);
             FileSystem fs = srcs.getFileSystem(job);
       
             List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
       
             Text key = new Text();
             PolicyInfo value = new PolicyInfo();
             SequenceFile.Reader in = null;
             long prev = 0L;
             int count = 0; // count src
             try {
               for (in = new SequenceFile.Reader(fs, srcs, job); in.next(key, value);) {//读输入文件
                 long curr = in.getPosition();
                 long delta = curr - prev;
                 if (++count > targetcount) {
                   count = 0;
                   splits.add(new FileSplit(srcs, prev, delta, (String[]) null));//一个EncodingCandidate一个map
                   prev = curr;
                 }
               }
             } finally {
               in.close();
             }
             long remaining = fs.getFileStatus(srcs).getLen() - prev;
             if (remaining != 0) {
               splits.add(new FileSplit(srcs, prev, remaining, (String[]) null));
             }
             LOG.info("jobname= " + jobName + " numSplits=" + numSplits + 
                      ", splits.size()=" + splits.size());
             return splits.toArray(new FileSplit[splits.size()]);
           }
       ```

       目录到最终mapTask的层级划分，一个encodingUnit会生成一个校验文件

    6. DistRaidMapper执行逻辑

       ![](D:\Thinker313\hadoop-20\note\doRaid.jpg)

    7. CorruptionWorker工作逻辑

       CorruptionWorker是定时触发扫描文件系统中丢失块的线程，它通过fsck获取。然后他会进行修块任务的提交.丢失块的文件可能是目录raid下的文件，普通raid文件或者是校验文件三种情况。三种情况执行逻辑如下：

       - 校验文件丢块
  
         ![](D:\Thinker313\hadoop-20\note\DirBlockFix.jpg)
  
         校验文件丢块没有过多依赖数据中存在信息，他的修复仍旧是通过计算校验文件丢失块对应的源文件条纹信息然后读取源文件进行解码修复。
  
       - raid目录下的文件丢块或者普通文件丢块
  
         这种情况与校验文件秀姑类似，只不过是丢失的块换成了源文件，要根据源文件计算到对应校验块，然后进行解码恢复。恢复过程与校验文件恢复类似
  
       - 目录重命名后有文件丢块，此时由于无法获取到对应的校验文件路径只能通过数据块进行重新构建。

    8. 
  
       
  
       3. ```
          srcPath=/home/yarn/data/test
          tmpDir=/tmp/jobID
          partialParityDir=/tmp/jobID/partialParityName
          finalPartialParityDir=/tmp/jobID/partialParityName/final
          tmpPartialParityDir==/tmp/jobID/partialParityName/tmp
          parityTmp=/tmp/jobID/test+randomLong,校验文件最先写入文件
          partialTmpParity=/tmp/jobID/partialParityName/tmp/currentStripeIdx ：完成校验文件写入后将其改名为此，一个encodingUnit会生成一个校验文件
          
          
          ```
  
    
  

