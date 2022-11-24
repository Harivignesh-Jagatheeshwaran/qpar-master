/Create tables if they do not exist

create_task:{
    t1:(`task_id`job_id`parent_task_ids`task_type`function_name`rhs`status`create_dt`start_dt`end_dt`lhs`slave_id`start_ts)!(0i;0i;(0i;0i);`$"test type";`$"test function";(0i;0j;"aaab");`initialize;.z.Z;.z.Z;.z.Z;(0i;0j;"aaab");0;.z.Z);
    tasks::1!enlist t1;
 };

create_slave_map:{
   t1:enlist (`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(`$"Test Slave";"2018b";"865a2a2";"create_tasks,test_addition,test_subtraction,test_division,square";100;0;"test_slave";"http://tesla.rxds.com/matlab/test_slave";`yes);
   t1:t1,(`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(`$"VP Slave";"2018b";"955b2a2";"create_vpparm,create_sim_tasks,run_ode";30;0;"vp_slave";"http://tesla.rxds.com/matlab/vp_slave";`yes);
  t1:t1,(`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(`$"Simbio";"2018b";"10cb2a9";"gen_input,run_sim,gen_output";30;0;"launch_slave";"http://tesla.rxds.com/matlab/simbio.tar.gz";`yes);
  t1:t1,(`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(`$"Nash";"2018b";"2.0";"gen_nash_input,run_nash,gen_nash_output";3;0;"launch_slave";"http://tesla.rxds.com/matlab/nash.tar.gz";`yes);
  t1:t1,(`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(`$"JuliaTestSlave";"julia1.2";"1.0";"julia_addition,julia_subtraction,julia_division,jsquare,julia_tasks";3;0;"launch_slave.jl";"http://tesla.rxds.com/julia/JuliaTestSlave.tar.gz";`yes);
  t1:t1,(`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(`$"InvalidEXE";"2018b";"df8569";"invalid_func";30;0;"invalid_exe";"http://tesla.rxds.com/matlab/invalid_exe";`yes);
  slave_map::1!t1;
  /--slave_map::`slave_type`version xkey t1;
 };

create_task_map: {
   t1:enlist (`task_type`default_instance`instance_type`slaves_per_instance`tasks_per_slave`instance_idle_time)!(`$"Small Task";`$"t3.medium";"t3.medium,t3.xlarge,t3.2xlarge";5;3;600);
   t1:t1,(`task_type`default_instance`instance_type`slaves_per_instance`tasks_per_slave`instance_idle_time)!(`$"Standard Task";`$"t3.medium";"t3.*,c3.*";3;3;600);
   t1:t1,(`task_type`default_instance`instance_type`slaves_per_instance`tasks_per_slave`instance_idle_time)!(`$"Medium Task";`$"t3.medium";"*.*";7;3;600);
   / - adding container entries for rach tasks;
   t1:t1,(`task_type`default_instance`instance_type`slaves_per_instance`tasks_per_slave`instance_idle_time)!(`$"Container Small Task";`$"container";"container";1;1;600);
   t1:t1,(`task_type`default_instance`instance_type`slaves_per_instance`tasks_per_slave`instance_idle_time)!(`$"Container Standard Task";`$"container";"container";1;1;600);
   t1:t1,(`task_type`default_instance`instance_type`slaves_per_instance`tasks_per_slave`instance_idle_time)!(`$"Container Task";`$"container";"container";1;1;600);
   
   task_instance::1!t1;
 };

create_instance_types: {
  instance_types::1!([]instance_type:();cpu_count:();max_slaves:());
  `instance_types insert (`$"t3.medium";2;2);
  `instance_types insert (`$"t3.xlarge";4;3);
  `instance_types insert (`$"t3.2xlarge";8;7);
 };

collect_stats: {
 s1:select stat:`slaves,stamp:.z.Z, status, num from select num:count i by status from slaves;
 n1:select stat:`nodes,stamp:.z.Z, status, num from select num:count i by status from nodes;
 t1:select stat:`tasks,stamp:.z.Z, status, num from select num:count i by status from tasks where task_id>0;
 qpar_stats::qpar_stats,s1,n1,t1;
 };

\c 100 200
@[value;"nodes";{nodes::1!([]node_id:();slave_port:();ami_id:();instance_id:();instance_type:();local_hostname:();ip:();ami_launch_index:();handle:();start_dt:();end_dt:();touch_dt:();status:();metrics:();spot_exit:();x_req:())}];
@[value;"slaves";{slaves::1!([]slave_id:();slave_port:();node_id:();pid:();handle:();ip:();user:();slave_type:();status:();job_id:();task_id:();start_dt:();end_dt:();touch_dt:();metrics:())}];
@[value;"tasks";create_task];
@[value;"jobs";{jobs::1!([]job_id:();job_type:();model_version:();start_dt:();status:())}];
@[value;"slave_map";create_slave_map];
@[value;"task_instance";create_task_map];
@[value;"qpar_stats";{qpar_stats::([]stat:();stamp:();status:();num:())}];

instance_types:1!("SSISSIIISSSSS";enlist",") 0:`:/rxds/matlab/ec2_instance_list.csv;
/Override max slaves for t3 2xlarge and c5 9x large instance types
/--update max_slaves:300 from `instance_types where instance_type like "c5.9xlarge";
/--update max_slaves:100 from `instance_types where instance_type like "t3.2xlarge";
/--update max_slaves:10 from `instance_types where instance_type like "t3.medium";
/--update tasks_per_slave:8 from `slave_map where slave_type = `Nash;

.rxds.max_nodes:5;
.rxds.USED:.z.P;
.rxds.port:system "p";
.rxds.cache:(`v1`v2)!(1;`x);
.rxds.current:0;
@[value;".rxds.use_node_capacity";{.rxds.use_node_capacity:1b}];
.rxds.stats_min:(`time`idle_time`active_since_last_run`fn)!(.rxds.qpar_stat_interval;0;1;collect_stats);
.rxds.cron:.rxds.cron,.rxds.stats_min;

                              
echo:{show x;x};
echo_test:{show x;show y;.rxds[x]:y;y};
/--function_map::2!ungroup select function_name:`${"," vs x}'[supported_functions], model_version:`$version, slave_type,tasks_per_slave from slave_map where is_active=`yes
function_map::2!ungroup select function_name:`${"," vs x}'[supported_functions], model_version:`$version, slave_type,tasks_per_slave from slave_map

.rxds.restart_tasks:1!flip (`task_id`retries)!((-1;0);(0;0));

restart_task:{[t]
  if[(count t)>0;
    t1:(exec task_id from t) except (exec task_id from .rxds.restart_tasks);
    .rxds.restart_tasks:.rxds.restart_tasks upsert select task_id,retries:0 from tasks where task_id in t1;
    update retries:retries+1 from `.rxds.restart_tasks where retries < 6;
    update status:`created,end_dt:{.z.Z}'[end_dt] from `tasks where task_id in (exec task_id from t),
        task_id in (exec task_id from .rxds.restart_tasks where retries<6);
    update status:`failed,end_dt:{.z.Z}'[end_dt] from `tasks where task_id in (exec task_id from t),
        task_id in (exec task_id from .rxds.restart_tasks where retries=6);
    /.rxds.restart_tasks:(exec `int$task_id from .rxds.restart_tasks),(exec task_id from t);
   /.rxds.restart_tasks:1!flip (`task_id`retries)!(((exec `int$task_id from .rxds.restart_tasks),(exec task_id from t));(0;0));
      /.rxds.restart_tasks:.rxds.restart_tasks upsert select task_id from t
   ];
   1
 };


reset_handle:{[s;h]
   t:exec count i from slaves where slave_port=s,handle=h;
   if[t>0;update handle:0Ni,touch_dt:.z.P from `slaves where slave_port=s,handle=h;:1];
   t:exec node_id from nodes where slave_port=s,handle=h;
   if[(count t)=0;:1];
   /update handle:0Ni,touch_dt:.z.P,status:`disconnected from `slaves where node_id in (t);
   /update status:`failed,end_dt:{.z.Z}'[end_dt]/,rhs:{enlist `node_exit}'[rhs]

  /s:select node_id, slave_id from slaves where node_id in (exec node_id from h);
  /t:select task_id,slave_id from tasks where slave_id in (exec slave_id from s), status in (`started;`working;`assigned);
   t1:select task_id from tasks where status in (`started;`working;`assigned)
    , slave_id in (exec slave_id from slaves where node_id in (t)) ;
   restart_task[t1];
   update handle:-2i,touch_dt:.z.P,status:`disconnected from `slaves where node_id in (t);
   update handle:-2i,status:`disconnected,end_dt:.z.P from `nodes where node_id in (t);
   1
 };


.z.pc:{
   .rxds.sh[.rxds.sh?x]:0; 
   1
 };

/Execute tasks evey 10 seconds 
.rxds.task_timer:10;
.rxds.cron:.rxds.cron,(`time`idle_time`active_since_last_run`fn)!(.rxds.task_timer;0;0;{matlab_exec_tasks[]});

/Execute waiting tasks every 1 seconds 
.rxds.task_timer:1;
.rxds.cron:.rxds.cron,(`time`idle_time`active_since_last_run`fn)!(.rxds.task_timer;0;0;{mat_updateJobsCompleted[];mat_informWaitingSlaves[];inform_waitingNodes[];});

/Launch nodes every 10 minutes 
.rxds.task_timer:300;
.rxds.cron:.rxds.cron,(`time`idle_time`active_since_last_run`fn)!(.rxds.task_timer;0;0;{launch_node[]});

/Retire idle slaves after 1 minute; Check every 30 seconds 
/Retire idle nodes after 2 minutes
.rxds.idle_check:30;
.rxds.slave_idle_timeout:300;
.rxds.slave_disconnect_after:300;
.rxds.node_idle_timeout:300;
.rxds.task_chunks:0;

.rxds.cron:.rxds.cron,(`time`idle_time`active_since_last_run`fn)!(.rxds.task_timer;0;0;{quit_idle_slave[];quit_idle_node[];disconnect_dead_slave[]});

get_handle:{[sp]
  if [.rxds.sh[sp]=0;.rxds.sh[sp]:hopen `$"" sv ("::";string sp)];
  .rxds.sh[sp]
 };

mat_sendMessage:{[message;h]
  sh:get_handle[h`slave_port];
  sh ("mat_sendMessage";message;h)
  show "In Send Message";
 };

quit_idle_slave:{
  h:select slave_port, handle from slaves where status=`idle, handle>0, (.z.P-touch_dt) > `timespan$1e9*.rxds.slave_disconnect_after;
  mat_sendMessage["quit"]'[h];
  1
 };

disconnect_dead_slave:{
  h:select slave_id,handle from slaves where status in (`launched`created`working`complete_task), (.z.P-touch_dt) > `timespan$1e9*.rxds.slave_idle_timeout;
  h:h,select slave_id,handle from slaves where status in (`idle), null handle, (.z.P-touch_dt) > `timespan$1e9*.rxds.slave_idle_timeout;
  update status:`disconnected, handle:-4i from `slaves where slave_id in (exec slave_id from h);
  update status:`disconnected, handle:-5i from `slaves where status=`idle, (0^handle)=0;
  1
 };


quit_idle_node:{
  h:select node_id, slave_port, handle, x_req from nodes where status=`process_wait, (.z.P-touch_dt) > `timespan$1e9*.rxds.node_idle_timeout;
  if[(count h)=0;:0];
  / Check if there are tasks on these nodes and tasks
  s:select node_id, slave_id from slaves where node_id in (exec node_id from h);
  t:select task_id,slave_id from tasks where slave_id in (exec slave_id from s), status in (`started;`working;`assigned);
  restart_task[t];
  h1:{sh:get_handle[x`slave_port];sh ("quit_node";x);}'[h];

  / Look for nodes that refused to disconnect 30 seconds after first try
  h1:select node_id, slave_port, handle, x_req from nodes where status=`process_wait, (.z.P-touch_dt) > (`timespan$1e9*.rxds.node_idle_timeout+30);
  update status:`disconnected, handle:-6i from `nodes where node_id in (exec node_id from h1);

  1
 };

 
quit_nodes:{[h]
  if[(count h)=0;:0];
  sh:get_handle[h`slave_port];
  sh ("quit_node";h);
  1
 };

mat_stopSlaves:{
   h:select from slaves;
   mat_sendMessage["quit"]'[h];
 };

init_node:{[x]
  show"<<< entering init_node >>>";
  .rxds.slave_handle::x;
  show x;
  sh:0;
  metrics:(`load`idle_cores`loaded_cores`totalmem`freemem`freemempct)!(0;0;0;0;0;0);
  node_id:{$[x[`c]=0;x`c;x`id]+1}[exec id:max node_id, c:count i from nodes];
  .rxds.qlog_port:.rxds.qlog_ports[node_id mod count .rxds.qlog_ports];
  qpar_port:.rxds.qpar_slave_ports[node_id mod count .rxds.qpar_slave_ports];
  / t:(`node_id`slave_port`ami_id`instance_id`instance_type`local_hostname`ip`ami_launch_index`handle`start_dt`end_dt`touch_dt`status`metrics`spot_exit`x_req)!
    /(node_id;qpar_port;x`ami_id;x`instance_id;`$x`instance_type;x`local_hostname;.z.a;x`ami_launch_index;sh;.z.P;0Np;.z.P;`node_register;metrics;`No;x`x_req);
  
  /update the slave container details including ip
  t:(`node_id`slave_port`ami_id`instance_id`instance_type`local_hostname`ip`ami_launch_index`handle`start_dt`end_dt`touch_dt`status`metrics`spot_exit`x_req)!
    (node_id;qpar_port;x`ami_id;x`instance_id;`$x`instance_type;x`local_hostname;x`local_hostname;x`ami_launch_index;sh;.z.P;0Np;.z.P;`node_register;metrics;`No;x`x_req);
  .rxds.debug::t;
  `nodes upsert t;
  max_slaves:exec max_slaves from instance_types where instance_type like x`instance_type;
  if[(count max_slaves)=0;show "New Instance type not configured"; show x`instance_type;max_slaves:enlist 2];
  (`node_id`max_slaves`qlog_port`qpar_port)!(node_id;max_slaves[0];.rxds.qlog_port;qpar_port)
  };


slave_exit:{[sp;x]
  .rxds.slave_log::x;
  /update handle:0Ni,status:`disconnected,end_dt:.z.P from `slaves where slave_id=x`slave_id;
  update handle:-1i,status:`disconnected,end_dt:.z.P from `slaves where slave_id=x`slave_id;
  t1:exec  count i from `tasks where slave_id=x`slave_id, status in (`started;`working;`assigned);
  if[t1=0;:1];
  update status:`failed,end_dt:{.z.Z}'[end_dt],rhs:{enlist `slave_exit}'[rhs]
  from `tasks where slave_id=x`slave_id, status in (`started;`working;`assigned);
  1
 };


launch_nodes:{[instance_type;instance_count]
    .rxds.command_str:raze "/rxds/scripts/launch_spot_fleet_new.sh '", (string instance_type), "' ", (string instance_count), " ", (string .rxds.port);
    show .rxds.command_str;
    system .rxds.command_str
 };

shutdown_nodes:{
    .rxds.command_str:"/rxds/scripts/shut_all.sh ";
    show .rxds.command_str;
    system .rxds.command_str
 };

/Launch nodes if  there are pending tasks
/****** This function needs to change to set a target for launching instead of actual launch
launch_node:{
 node_count:exec count i from nodes where  status = `process_wait;
 if[node_count>=.rxds.max_nodes;:0];
 / Are there jobs
 j:exec job_id from jobs where status=`submitted;
 if[(count j)=0;:0]; /No jobs available - stop
 / Now find tasks ready to run
 pid:select from (ungroup select task_id, parent_task_ids from tasks where status=`created,job_id in (j)) where parent_task_ids > 0;
 incomplete_pids:exec task_id from tasks where status <> `completed,task_id in (exec parent_task_ids from pid);
 not_ready:exec task_id from pid where parent_task_ids in (incomplete_pids);
 h1:`task_id xasc select job_id,task_id,task_type,function_name,rhs from tasks where status=`created,job_id in(j),not task_id in (not_ready);
 if[(count h1)=0;:0]; /No tasks available
 h2:select task_count:count i by task_type from h1;
 h2:0!select instance_count:`int$(0.5 + sum instance_count) by instance_type from 
    select task_type, instance_type:default_instance, instance_count:(task_count%tasks_per_slave)%slaves_per_instance 
    from h2 lj task_instance;
  
  {[node_count;x]
    icount:x`instance_count; 
    if[(node_count+icount)>.rxds.max_nodes;icount:(.rxds.max_nodes-node_count)];
    if[icount<=0;:0];
    node_count:node_count+icount;
    .rxds.command_str:raze "/rxds/matlab/launch.sh '", (string x`instance_type), "' ", (string x`instance_count);
    show .rxds.command_str;
    show "Launching disabled temporarily";
    /system .rxds.command_str;
   }[node_count]'[h2];
  count h2
 };

node_stats:{[sp;x]
  .rxds.node_stats:x;
  update metrics:enlist x`metrics from `nodes where node_id=x`node_id;
  1
 }; 

slave_stats:{[sp;x]
  .rxds.slave_stats:x;
  /Stop updating metrics if the slave is just waiting for tasks
  c1:exec count i from `slaves where slave_id=x`slave_id, not status in (`idle);
  if [(c1=0);:1];
  update metrics:enlist x`metrics,touch_dt:.z.P from `slaves where slave_id=x`slave_id, not status=`idle;
  update touch_dt:.z.P from `nodes where node_id=x`node_id;
  1
 }; 

node_spot_terminate:{[sp;x]
  -1"Entering node spot terminate: ",string x`node_id;
  update spot_exit:`Yes, touch_dt:.z.P from `nodes where node_id=x`node_id;
  1
 };

/New function to see if a node will launch slaves
check_node_slave:{[pnode_id]
  wait:(`x_wait`node_id`reason)!(`Yes;pnode_id;`init);

  node_slave_count:exec count i from slaves where node_id = pnode_id, not status =`disconnected;
  node_parm:exec from nodes where node_id=pnode_id;
  max_slaves:exec max_slaves from instance_types where instance_type = node_parm`instance_type;
  /update status:`process_wait,slave_port:sp,handle:sh,x_req:x`x_req from `nodes where node_id=x`node_id;
  if[node_slave_count>=max_slaves[0];wait[`reason]:`NoCapacityInNode;:wait]; /No more capacity on this node wait

  task_types:exec task_type from task_instance where {x:string x;y:"," vs y;any x like/:y}[node_parm`instance_type]'[instance_type];

  / Are there jobs
  j:exec job_id from jobs where status=`submitted;
  if[(count j)=0;wait[`reason]:`NoJobs;:wait]; /No jobs available - wait
  / Now find tasks ready to run
  pid:select from (ungroup select task_id, parent_task_ids from tasks where status=`created,job_id in (j)) where parent_task_ids > 0;
  incomplete_pids:exec task_id from tasks where status <> `completed,task_id in (exec parent_task_ids from pid);
  not_ready:exec task_id from pid where parent_task_ids in (incomplete_pids);
  /h1:`task_id xasc select job_id,task_id,task_type,function_name,rhs from tasks where status=`created,job_id in(j),task_type in (task_types), not task_id in (not_ready);
  m:raze exec job_id,model_version from jobs where status=`submitted;
  h1:`task_id xasc select job_id,model_version:m[job_id],task_id,task_type,function_name,rhs from tasks where status=`created,job_id in (j), task_type in (task_types), not task_id in (not_ready);
  if[(count h1)=0;wait[`reason]:`NoTasks;:wait]; /No tasks available

  /Identify what slave is needed for these tasks
  s:`task_count xdesc select task_count:count i, tasks_per_slave:max tasks_per_slave by slave_type from
      select job_id, model_version, task_id, slave_type,tasks_per_slave
      from h1 lj function_map;
  if[(count s)=0;wait[`reason]:`NoSlaveMapped;:wait]; /No slaves  mapped yet
  s:`task_count xdesc select slave_type,task_count,tasks_per_slave,slave_count:`int$(task_count%tasks_per_slave+0.5) from s;

  /If we want to reserve slave capacity
  if[not .rxds.use_node_capacity;
     /Count slaves  running by slave type across all nodes
     run_slave_count:select run_slave_count:count i by slave_type from slaves where not status =`disconnected;
     s:`task_count xdesc select from (s lj run_slave_count) where slave_count>run_slave_count;
     if[(count s)=0;wait[`reason]:`SlavesEnough;:wait]; /No slaves  needed yet
  ];

  /Pick first row
  s:select from s where i=0;

  sm:1!select slave_type, runtime, version, executable, exec_path from slave_map;
  s:(s lj sm)[0];
  s[`slave_count] = s[`slave_count] - node_slave_count;
  s[`master_ip]:.rxds.master_ip;
  s[`master_port]:0;
  s[`slave_id]:9999;
  show s;
  (`x_wait`slave)!(`No;s)
  }; / check_node_slave


/ Tell the node to launch a slave process if needed
process_listen:{[sp;sh;x]
  /show ">>>>>>>>>>>>>..  sp : ", string sp;
  /show ">>>>>>>>>>>>>..  sh : ", string sh;
  / show ">>>>>>>>>>>>>..  x : ", string x;
  show " >>>>>>>> enterting process_listen >>>>>>> ";
  show x;
  .rxds.slave_handle::x;
  /show .rxds.slave_handle;
  wait:(`x_wait`node_id`reason)!(`Yes;x`node_id;`init);

  show " >>>> node_slave_cnt : ", string node_slave_count:exec count i from slaves where node_id = x`node_id, not status =`disconnected;
  show  node_parm:exec from nodes where node_id=x`node_id;
/  show " >>>> max_slaves : ", string max_slaves;
 max_slaves:exec max_slaves from instance_types where instance_type = node_parm`instance_type;
  / max_slaves:exec max_slaves from instance_types where instance_type = `$.rxds.slave_handle[`instance_type];
  update status:`process_wait,slave_port:sp,handle:sh,x_req:x`x_req from `nodes where node_id=x`node_id; 
  show node_slave_count; show max_slaves[0];
  if[node_slave_count>=max_slaves[0];wait[`reason]:`NoCapacityInNode;:wait]; /No more capacity on this node wait
 show " >>> task_instance - table >>> "; show task_instance;
 show node_parm; show type node_parm`instance_type; 

  task_types:exec task_type from task_instance where {x:string x;y:"," vs y;any x like/:y}[node_parm`instance_type]'[instance_type];
    
  / Are there jobs
  j:exec job_id from jobs where status=`submitted;
  if[(count j)=0;wait[`reason]:`NoJobs;:wait]; /No jobs available - wait
  / Now find tasks ready to run
  pid:select from (ungroup select task_id, parent_task_ids from tasks where status=`created,job_id in (j)) where parent_task_ids > 0;
  incomplete_pids:exec task_id from tasks where status <> `completed,task_id in (exec parent_task_ids from pid);
  not_ready:exec task_id from pid where parent_task_ids in (incomplete_pids);
  m:1!select job_id,model_version,job_type,status from jobs where status=`submitted;
  /h1:`task_id xasc select job_id,task_id,task_type,function_name,rhs from tasks where status=`created,job_id in(j),task_type in (task_types), not task_id in (not_ready);
  h1:lj[select from tasks where status=`created,task_type in (task_types), not task_id in (not_ready);m];
  h1:`task_id xasc select job_id,model_version,task_id,task_type,function_name,rhs from h1 ;
  show " >>>> m table - submitted jobs";
  show m; show " >>> h1 table - created tasks "; show h1;
  if[(count h1)=0;wait[`reason]:`NoTasks;:wait]; /No tasks available

  /Identify what slave is needed for these tasks
  s:ej[`function_name`model_version;select function_name,model_version, job_id, task_id from h1;function_map];
  /show s;
  if[(count s)=0;wait[`reason]:`NoSlaveMapped;:wait]; /No slaves  mapped yet
  s1:`task_count xdesc select task_count:count i, tasks_per_slave:max tasks_per_slave by slave_type from s;
  /show s1;
  /--if[(count s)=0;wait[`reason]:`NoSlaveMapped;:wait]; /No slaves  mapped yet
  s:`task_count xdesc select slave_type,task_count,tasks_per_slave,slave_count:`int$(task_count%tasks_per_slave+0.5) from s1;

  /If we want to reserve slave capacity
  if[not .rxds.use_node_capacity;
     /Count slaves  running by slave type across all nodes
     run_slave_count:select run_slave_count:count i by slave_type from slaves where not status =`disconnected;
     s:`task_count xdesc select from (s lj run_slave_count) where slave_count>run_slave_count;
     if[(count s)=0;wait[`reason]:`SlavesEnough;:wait]; /No slaves  needed yet
  ];
  
  /Pick first row
  s:select from s where i=0;

  sm:1!select slave_type, runtime, version, executable, exec_path from slave_map;
  s:(s lj sm)[0];      
  s[`slave_count] = s[`slave_count] - node_slave_count;
  s[`master_ip]:.rxds.master_ip;
  s[`master_port]:sp;
  show "node id >>>>>>>>>>>>>>>>>>>>>>>>>>> ",string x`node_id;
  s[`slave_id]:mat_createSlave[sp;sh;s[`slave_type];x`node_id];
  show s;
  (`x_wait`slave)!(`No;s)   
  };

inform_waitingNode:{[node]
     slave:process_listen[node[`slave_port];node[`handle];node];
     if [(slave[`x_wait])=`Yes; :0];
     sh:get_handle[node`slave_port];
     show sh;
     if[0=sh;:`$"disconnected spot node"];
     //if[((key .z.W)?sh)=count .z.W;:`$"node spot terminate"];
     sh ("inform_waitingNode";node;slave);
     1
 };

inform_waitingNodes:{
    r1:select slave_count:count i by node_id from slaves where not status = `disconnected;
    r2:select from nodes lj r1 where status in (`node_register`process_wait);
    r3:0!select from r2 lj instance_types where (0^slave_count) < max_slaves;
     if[(count r3)=0;:0]; /No nodes with capacity return
  / Are there jobs
  j:exec job_id from jobs where status=`submitted;
  if[(count j)=0;:0]; /No jobs return
    inform_waitingNode'[r3]; 
    1
 };

mat_warmUpNodes:{[sp;p_task_type;p_task_count]
  h:exec instance_type:default_instance, instance_count:`int$(p_task_count%tasks_per_slave)%slaves_per_instance from task_instance 
    where  task_type=p_task_type;
  .rxds.command_str:raze "/rxds/matlab/launch.sh '", (string h`instance_type), "' ", (string h`instance_count);
  show .rxds.command_str;
  system .rxds.command_str;
 };

mat_createSlave:{[sp;sh;p_slave_type;p_node_id]
  /show  p_slave_type; 
  show ">>>>>>>>>>>>> p_node_id : ", string p_node_id;
  .rxds.s:p_slave_type;
  p_slave_id:{$[x[`c]=0;x`c;x`id]+1}[exec id:max slave_id, c:count i from slaves];
  metrics:(`proc`vsize`cpu)!(0;0;0);
  t:(`slave_id`slave_port`node_id`pid`handle`ip`user`slave_type`status`job_id`task_id`start_dt`end_dt`touch_dt`metrics)!(p_slave_id;sp;`int$p_node_id;0i;sh;.z.a;.z.u;p_slave_type;`created;enlist 0i;enlist 0i;.z.P;0Np;.z.P;metrics);
  .rxds.T:t;
 `slaves upsert t;
  show t;
  p_slave_id
 };

mat_updateSlave:{[sp;sh;p_slave_id;p_pid]
  update pid:p_pid,status:`launched,touch_dt:.z.P,slave_port:sp,handle:sh from `slaves where slave_id=p_slave_id;
  .rxds.qio_port:.rxds.qio_ports[p_slave_id mod count .rxds.qio_ports];
  .rxds.qtrace_port:.rxds.qtrace_ports[p_slave_id mod count .rxds.qtrace_ports];
  .rxds.qpar_lives_port:.rxds.qpar_lives_ports[p_slave_id mod count .rxds.qpar_lives_ports];
  tt:(`qio_port`qtrace_port`qpar_live_port)!(.rxds.qio_port;.rxds.qtrace_port;.rxds.qpar_lives_port);
  tt
 };

mat_registerSlave:{[sp;sh;p_slave_id;p_wait_type;p_job_id;p_task_id]
 t:slaves[p_slave_id];
 t[`slave_id]:p_slave_id;
 t[`slave_port]:sp;
 t[`job_id]:$[(type p_job_id)<0;enlist `int$p_job_id;`int$(p_job_id)];
 t[`task_id]:$[(type p_task_id)<0;enlist `int$p_task_id;`int$(p_task_id)];
 t[`handle]:sh;
 t[`status]:p_wait_type;
 t[`touch_dt]:.z.P;
  update touch_dt:.z.P from `nodes where node_id in (exec node_id from slaves where slave_id=`int$p_slave_id);
 `slaves upsert t;
 `registered
 };


mat_uploadSlaveOutput:{[sp;sh;p_slave_id;p_job_id;p_task;p_output]
    update lhs:p_output,status:`completed,end_dt:.z.Z from `tasks where task_id=p_task,job_id=p_job_id;
    update status:`complete_task,end_dt:.z.P,handle:sh from `slaves where slave_id=p_slave_id;
    1
 };

mat_updateExecutable:{[sp;p_name;p_runtime;p_version;p_functions;p_exec;p_path;p_status]
  t1:(`slave_type`runtime`version`supported_functions`tasks_per_slave`slave_count`executable`exec_path`is_active)!(p_name;string p_runtime;string p_version;string p_functions;1;0;string p_exec;string p_path;p_status);
  `slave_map upsert t1;
  1
 };

mat_updateIdleJob:{[sp;p_jobid]
 t1:exec job_id from `jobs where job_id=p_jobid,status=`submitted;
 update status:`completed from `tasks where job_id in t1, status=`created;
 1
 };

mat_updateTaskStatus:{[sp;p_jobid;p_current;p_tobe]
 t1:exec job_id from `jobs where job_id=p_jobid,status=`submitted;
 update status:p_tobe from `tasks where job_id in t1, status=p_current;
 1
 };

/ Check for slaves waiting for this task
/s:exec handle from slaves;
mat_informWaitingSlaves:{
  jobList:select handle,job_id,slave_port from (ungroup select handle,job_id,slave_port from slaves where status=`idle) where job_id>0;
  taskList:select handle,task_id,slave_port from (ungroup select handle,task_id,slave_port from slaves where status=`idle) where task_id>0;
  if[(count jobs) > 0;
     jobs_complete:exec job_id from jobs where status = `completed,job_id in (exec job_id from jobList);
     incomplete_jobs:exec job_id from jobs where status <> `completed,job_id in (exec job_id from jobList);
     not_ready:exec handle from jobList where job_id in (incomplete_jobs);
     ready:select distinct handle,slave_port from jobList where job_id in (jobs_complete), not handle in (not_ready);
     mat_sendMessage[`jobs_completed]'[ready];
     ];
  if[(count tasks) > 0;
     task_complete:exec task_id from tasks where status in (`completed`failed),task_id in (exec task_id from taskList);
     incomplete_tasks:exec task_id from tasks where not status in (`completed`failed),task_id in (exec task_id from taskList);
     not_ready:exec handle from taskList where task_id in (incomplete_tasks);
     ready:select distinct handle,slave_port from taskList where task_id in (task_complete), not handle in (not_ready);
     mat_sendMessage[`jobs_completed]'[ready];
     ];
 };

mat_updateJobsCompleted:{
  j:exec job_id from jobs where status=`submitted;
  t:exec job_id from tasks where job_id in (j), not status  in (`completed`failed);
  update status:`completed from `jobs where job_id in (j except t);
  1
 };
mat_setSlaveTaskFailure:{[sp;sh;p_slave_id;p_job_id;p_task;p_output]
    update lhs:enlist p_output,status:`failed,end_dt:.z.Z from `tasks where task_id=p_task;
    update status:`failed_task,end_dt:.z.P,handle:sh from `slaves where slave_id=p_slave_id;
    1
 };

mat_createJob:{[sp;p_job_type;p_version]
   show p_job_type;
   p_job_id:(count jobs)+1;
   t:(`job_id`job_type`model_version`start_dt`status)!(p_job_id;p_job_type;p_version;.z.Z;`created);
   `jobs upsert t;
   p_job_id
 };

mat_createTask:{[sp;p_job_id;p_parent_tasks;p_task_type;p_function;p_rhs]
   p_task_id:(count tasks)+1;
   p_parent_task_id:$[(type p_parent_tasks)<0;enlist `int$p_parent_tasks;`int$(p_parent_tasks)];
   p_rhs:$[(type p_rhs)<0;enlist p_rhs;p_rhs];
   t:(`task_id`job_id`parent_task_ids`task_type`function_name`rhs`status`create_dt`start_dt`end_dt`lhs`slave_id)!(p_task_id;p_job_id;p_parent_task_id;p_task_type;p_function;p_rhs;`created;.z.Z;.z.Z;.z.Z;(0i;0j;"aaab");sp);
   `tasks upsert t;
   p_task_id
 };

mat_scatterTaskParm:{[sp;p_job_id;p_parent_tasks;p_task_type;p_function;p_rhs_list]
   p_task_id_list:mat_createTask[sp;p_job_id;p_parent_tasks;p_task_type;p_function]'[p_rhs_list];
   p_task_id_list
 };


mat_submit:{[sp;p_job_id]
   update status:`submitted from `jobs where job_id=p_job_id,status=`created;
   p_job_id
 };

mat_taskGather:{[sp;p_job_id;p_task_id]
   if [p_task_id=0;
     :exec task_id, status, start_dt, end_dt, lhs from tasks where job_id = p_job_id;
   ];
   exec task_id, status, start_dt, end_dt, lhs from tasks where task_id in (p_task_id)
 };

mat_taskReduce:{[sp;p_job_id;reduce_func]
   l:exec lhs from tasks where job_id = p_job_id, status=`completed;
   r:parse reduce_func;
   @[r;l;{"Error with reduce function : ",x}]
 };


mat_startTask:{[sp;sh;p_slave_id;p_job_id;p_task_id]
  update status:`started,start_ts:.z.Z from `tasks where task_id=p_task_id,job_id=p_job_id;
  update status:`working,handle:sh from `slaves where slave_id = p_slave_id;
  1
 };


matlab_getTask:{[sp;sh;p_slave_type;p_slave_id]
  p_slave_id:`long$p_slave_id;
  .rxds.s:p_slave_type;.rxds.s1:p_slave_id;
  /show p_slave_type;
  /show .rxds.cache;
  /Is there a cache
  if[(type .rxds.cache[p_slave_type])=-7h;tcount:0];
  if[(type .rxds.cache[p_slave_type])=98h;tcount:count .rxds.cache[p_slave_type]];
  
  if[ .rxds.current >= tcount;
    fn:exec supported_functions from slave_map where slave_type = p_slave_type;
    fn:"," vs fn[0];
    j1:select job_id,model_version from jobs where status=`submitted;
    if[(count j1)=0;:(`task_id`job_id)!(0;0)]; /No jobs available
    j:exec job_id from j1;
  
    pid:select from (ungroup select task_id, parent_task_ids from tasks where any (string function_name) like/: fn, status=`created,job_id in (j)) where parent_task_ids > 0;
    incomplete_pids:exec task_id from tasks where status <> `completed,task_id in (exec parent_task_ids from pid);
    not_ready:exec task_id from pid where parent_task_ids in (incomplete_pids);
  
    h1:`task_id xasc select job_id,task_id,task_type,function_name,rhs from tasks where any function_name like/: fn, status=`created,job_id in(j),not task_id in (not_ready);
    if[(count h1)=0;:(`task_id`job_id)!(0;j)]; /No slaves available
    .rxds.cache[p_slave_type]:h1;
    .rxds.current:0;
  ];
  
  update status:`working, touch_dt:.z.P  from `slaves where slave_id=`int$p_slave_id;
  if[sh > 0; update slave_port:sp,handle:sh from `slaves where slave_id=`int$p_slave_id]
  
  update touch_dt:.z.P from `nodes where node_id in (exec node_id from slaves where slave_id=`int$p_slave_id);
  h:select from .rxds.cache[p_slave_type] where i within (.rxds.current;.rxds.current+.rxds.task_chunks);
  .rxds.current:.rxds.current+.rxds.task_chunks+1;
  update status:`assigned,start_dt:.z.Z,slave_id:p_slave_id from `tasks where task_id in (exec task_id from h);
  if[(count pid)=0;:h]; /parent table is empty
  pid:select task_id, parent_task_ids from pid where task_id in (exec task_id from h);
  if[(count pid)=0;:h]; /parent table is empty for these tasks
  h:{[pid;h]
     show "finding parent output";
     show pid;
     show h;
     parent_output:select task_id, lhs from tasks where task_id in (exec parent_task_ids from pid where task_id=h[`task_id]);
     if[(count parent_output)=0;:h]; /No parent dependency
     t1:raze(h`rhs;raze exec lhs from parent_output);
     show t1;
     h[`rhs]:t1;
     show h;
     h
   }[pid]'[h];
   h
 };

matlab_exec_task:{
  s1:`handle xasc select from slaves where status=`idle; 
  s:exec from s1 where i=0;
  if[(count s1)=0;:0]; /No slaves available
  h:matlab_getTask[s`slave_port;0;s`slave_type;s`slave_id];

  /No tasks available
  if[((type h)<>98);if[(h`task_id)=0;:0]];
  show h;
  mat_sendMessage[h;s];
  /neg[s`handle] h;
  count h
 };

matlab_exec_tasks:{
  h:matlab_exec_task[];
  while[h>0;h:matlab_exec_task[]];
  0
 }

get_task: { :select from tasks };

get_task_t:{ a:exec count i by status from (0!select from tasks); 
  if [(not `submited  in key a);a[`submited]:0];
  if [(not `completed in key a);a[`completed]:0];
  if [(not `created in key a);a[`created]:0] ;
  :a};

