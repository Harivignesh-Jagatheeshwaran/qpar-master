/- Updated on 01/06/2021
show "Loading Latest Version"
/- Tested on Matlab server for segmented partition
\c 200 500

/Flush to disk if there is a pause
.rxds.task_timer:10;
delete last_run from `.rxds.cron;
.rxds.cron:.rxds.cron,(`time`idle_time`active_since_last_run`fn)!(60;120;0;{flush_to_disk[]});
.rxds.cached_tables:();

DBPATH::hsym[`$.rxds.IMDB]

/-- hsym[`$.rxds.IMDB,"/par.txt"] 0: (string each .rxds.qio_segments)
nop::.rxds.part_by

loadtables:{[p_table]
 /-send_to_ports["@[.Q.bv[`];{show \"bv failed\"}]"]
 @[.Q.bv;[`];{show "bv [`] failed";.Q.bv[]}];
 send_to_ports["@[.Q.bv;[`];{show \"bv [`] failed\";.Q.bv[]}]"];
 :`$("Loaded ", string p_table)
 }

jul_createTable:{[p_namespace;p_table;p_stor;p_name;p_vals;p_keys]
 p_cols:p_name!p_vals;
 p_namespace:`symbol$();
 mat_createTable[p_namespace;p_table;p_stor;p_cols;p_keys]
 }

mat_createTable:{[p_namespace;p_table;p_stor;p_cols;p_keys]
 .rxds.p_cols:p_cols;
 /- hsym[`$.rxds.IMDB,"/par.txt"] 0: (string each ((.rxds.qio_segments),(.rxds.hist_segments)));
 hsym[`$.rxds.IMDB,"/par.txt"] 0: string each .rxds.qio_segments;
 t:tab[p_namespace;p_table];
 t1:`$t;
 mt:tab[`symbol$();`meta_table];
 mt1:`$mt;
 /- Load meta table from the disk if it exists, if not create new
 t:.[ld;(`symbol$();`meta_table);1b];
 $[t~1b;@[value;mt;create_metatable];mt1 set t];
 /- @[value;mt;create_metatable];
 $[1=count p_keys;
  [mt1 upsert(t1;p_stor;exec c from meta enlist p_cols;enlist p_keys;trim exec t from meta enlist p_cols;.z.Z)];
  [mt1 upsert(t1;p_stor;exec c from meta enlist p_cols;p_keys;trim exec t from meta enlist p_cols;.z.Z)]];

 cd[`symbol$();`meta_table];
 show `MetaUpserted;
 /--:`$"Meta Created, Table Not Seeded";
 /- check if the incoming payload has data or just the structure
 s:trim exec t from meta enlist p_cols;
 typ:(trim exec typ from  meta_table where tab=t1)[0];
 
 /-show s;
 /-show typ;
 /-- flip will tell you if the incoming structure can be flipped to a table
 /-- flip will fail on a structure with attributes of single value, but still is a candidate for single row table, hence checking count
 /-- if the incoming structure has attributes with different number of values, it is a candidate for struct2cell
 /-op_flag:`true;
 /-if[(count flip p_cols)~(max count each p_cols);op_flag:`true;op_flag:`false];
 /-show op_flag; 


 @[flip;p_cols;.rxds.op_flag:$[1=(*/) (max count each p_cols)=':(count each p_cols);`true;`false]];
 if[.rxds.op_flag=`true;
   /-if it is single ontainer, so we use enlist instead of flip for p_cols (di)
   if[(""~typ) and 0<count enlist p_cols;:`$"Each vector in the struct must be of same length"]; 
   if[(""~typ) and 0=count enlist p_cols;:`$"Mcdeta has been created"];

   if[(not ""~s);
     -1"Calling struct2row";
     mat_struct2row[p_namespace;p_table;`immediate;p_cols];
    :`TableSeeded]
  ];
   
 if[.rxds.op_flag=`false;
   if[(not ""~s);
     -1"Calling struct2cell";
     mat_struct2cell[p_namespace;p_table;`immediate;p_cols];
    :`TableSeeded
   ]
 ];
 /--  show ""~s;
 /-- show ""~typ;
 :`$"Meta Created, Table Not Seeded"
 }

create_metatable:{
 t:`$x;
 t1:1!flip[`tab`stor`col`pk`typ`stamp!"ss***z"$\:()];
 t set t1;
 `MetaTableCreated
 }

jul_struct2row:{[p_namespace;p_table;p_ttype;p_name;p_vals]
 p_data:p_name!p_vals;
 p_namespace:`symbol$();
 mat_struct2row[p_namespace;p_table;p_ttype;p_data]
 }

mat_struct2row:{[p_namespace;p_table;p_ttype;p_data]
 s:trim exec t from meta enlist p_data;
 if[""~s;:`$"Each vector in the struct must be of same length"];
 load `meta_table;
 .rxds.D::p_data;
 .rxds.USED:.z.P;
 /-rt:system("pwd");
 t:tab[p_namespace;p_table];
 t1:`$t;
 /- Check for the table definition exists in meta_table
 c:exec count i from meta_table where tab like t;
 /- Determine the storage type
 st:(exec stor from meta_table where tab like t)[0];
 /- Check  the data type
 typ:(trim exec typ from  meta_table where tab like t)[0];
 /- If the meta has been created without data, there will not be a data type so update it
 if[""~typ;
   -1"Updating data type of ",t;
   @[flip;.rxds.D;{-1"Each vector in the struct must be of same length";:1}];
   typ:$[1<count flip .rxds.D;exec t from meta flip .rxds.D;exec t from meta enlist .rxds.D];
   `meta_table upsert ([tab:enlist t1]typ:enlist typ);
   cd[`symbol$();`meta_table];
   send_to_ports["system \"l meta_table\""];
   -1"Meta table refreshed across all ports"
  ];
 if[0<c;
   [@[value;t;init_setup];
   /- Check datatype and columns of the incoming payload for the table
   if[(1b~check_struct[p_data;t]);
    [
      /- Check the storage type
    if[st~`memory;
    /- Determine if the incoming struct has vector of values to adjust the upsert
     [$[1<max count each p_data;
     [p_data[`stamp]:.z.Z;t1 upsert flip p_data;`memmatched];
     [p_data:p_data,(enlist `stamp)!enlist .z.Z;t1 upsert select from enlist p_data;`memmatched]]]
     ];
    /- Check the storage type
    if[st~`splayed;
      /- Determine if the incoming struct has vector of values to adjust the upsert
      p_data:@[flip;p_data;enlist p_data];
      p_data[`stamp]:.z.Z;splay_append[t;p_data];
     ];
    /- Check the storage type
    if[st~`partition;
      p_data:@[flip;p_data;enlist p_data];
      p_data[`stamp]:.z.Z;splay_part[t;p_data;p_ttype];
      :`$"Data logged";
     ];
   ];
    `structmismatch]];
   :`$("No such table exists =>",string t1)];
 `$"Data logged"
 }

mat_struct2cell:{[p_namespace;p_table;p_ttype;p_data]
 s:trim exec t from meta enlist p_data;
 if[""~s;:`$"Attribute values in struct has to be a row vector"];
 load `meta_table;
 .rxds.D::p_data;
 .rxds.USED:.z.P;
 /-rt:system("pwd");
 t:tab[p_namespace;p_table];
 t1:`$t;
 /- Check for the table definition exists in meta_table
 c:exec count i from meta_table where tab like t;
 /- Determine the storage type
 st:(exec stor from meta_table where tab like t)[0];
 /- Check  the data type
 typ:(trim exec typ from  meta_table where tab like t)[0];
 /- If the meta has been created without data, there will not be a data type so update it
 if[""~typ;
   -1"Updating data type of ",t;
   /-- typ:exec t from meta enlist .rxds.D;
   /--typ:$[1<count flip .rxds.D;exec t from meta flip .rxds.D;exec t from meta enlist .rxds.D];
   typ:exec t from meta enlist .rxds.D;
   `meta_table upsert ([tab:enlist t1]typ:enlist typ);
   cd[`symbol$();`meta_table];
   send_to_ports["system \"l meta_table\""]
  ];
 $[c>0;
   [@[value;t;init_setup];
   /- Check datatype and columns of the incoming payload for the table
   $[check_struct[p_data;t];
    /- Append timestamp at the end of dict
    [
      /- Check the storage type
    if[st~`memory;
    /- Determine if the incoming struct has vector of values to adjust the upsert
     [$[1<max count each p_data;
     [p_data[`stamp]:.z.Z;t1 upsert flip p_data;`memmatched];
     [p_data:p_data,(enlist `stamp)!enlist .z.Z;t1 upsert select from enlist p_data;`memmatched]]]
     ];
    /- Check the storage type
    if[st~`splayed;
      /- Determine if the incoming struct has vector of values to adjust the upsert
      p_data:@[flip;p_data;enlist p_data];
      p_data[`stamp]:.z.Z;splay_append[t;p_data];
     ];
    /- Check the storage type
    if[st~`partition;
      /--p_data:@[flip;p_data;enlist p_data];
      p_data:enlist p_data;
      p_data[`stamp]:.z.Z;splay_part[t;p_data;p_ttype];
      :`$"Data logged";
     ];
   ];
    :`structmismatch]];
  :`$("No such table exists =>",string t1)];
 :`$"Data logged"
 }

splay_part:{[tn;recs;ttype] 
 /-rt:system("pwd");
 
 .rxds.D::recs;
 pk:(exec pk from meta_table where tab like tn)[0];
 partkey:1#pk;
 part:value(value(getPartKey;partkey;`$".rxds.D"));
 paths:{createPartPath[y;x]}[`$tn;] each part;
 /- show paths;
 if [ttype~`immediate;splay_part_write[tn;pk;partkey;part;paths];:1];

   -1"About to execute in-memory caching";
   /-show part;
   p_qio_port:.rxds.qio_master_ports[floor(part[0]%nop) mod count .rxds.qio_master_ports];
     show p_qio_port;
     if [.rxds.port <> p_qio_port; 
          qh:get_handle[p_qio_port];
          out:neg[qh] ("splay_part_inmem";tn;.rxds.D;pk;partkey;part;paths);
          :out];
    splay_part_inmem[tn;.rxds.D;pk;partkey;part;paths]
     
 }

splay_part_inmem:{[tn;recs;pk;partkey;part;paths]
     -1"Executing in-memory caching";
     .rxds.D::recs;
     cached_tables:@[get;".rxds.cached_tables";()];
     if[not (`$tn) in cached_tables; .rxds.cached_tables:cached_tables,`$tn;];

     memkey:@[get;".rxds.part",tn;part];
     inmemtab:@[get;".rxds.",tn;delete from .rxds.D];
     /-show part[0];
     /if[memkey[0] <> part[0]; 
     if[(count inmemtab) > .rxds.inmemlimit;
          .rxds.D::inmemtab;
          part:value(value(getPartKey;partkey;`$".rxds.D"));
          paths:{createPartPath[y;x]}[`$tn;] each part;
          splay_part_write[tn;pk;partkey;memkey;paths];
          memkey:part;
          .rxds:(`$tn) _ .rxds; / Delete in memory table 
          /-- Reset to new table
          .rxds.D::recs;
          inmemtab:delete from .rxds.D;
     ];
     inmemtab:inmemtab,.rxds.D;
     (`$(".rxds.",tn)) set inmemtab;
     (`$(".rxds.part",tn)) set memkey;
     part
 }

flush_table:{
  tn:string x;
  inmemtab:@[get;".rxds.",tn;delete from .rxds.D];
  .rxds.D::inmemtab;
  show "flushing to disk";
  show tn;
  pk:(exec pk from meta_table where tab like tn)[0];
  show pk;
  partkey:1#pk;
  part:value(value(getPartKey;partkey;`$".rxds.D"));
  paths:{createPartPath[y;x]}[`$tn;] each part;
  splay_part_write[tn;pk;partkey;part;paths];
  .rxds.cached_tables:.rxds.cached_tables except x;
  .rxds:(`$tn) _ .rxds; /  Delete the in memory table that has been flushed
  .rxds:(`$"part",tn) _ .rxds; /Delete the partition key in memory

  1
  }

flush_to_disk:{flush_table each .rxds.cached_tables;0};

write_part:{[tn;path;eTab]
  new_folder:()~key path;
  if[new_folder; 
     show  "new partition";
     path set  eTab; :0;
     ];
  path upsert eTab; 
  1
 };

splay_part_write:{[tn;pk;partkey;part;paths]
 /exists:@[value;tn;`no];
 show tn;
 if[0=count .rxds.D;:1];
 part:value(value(getPartKey;partkey;`$".rxds.D"));
 paths:{createPartPath[y;x]}[`$tn;] each part;
 range:value(value(getPartRange;partkey;`.rxds.D));
 eTabs:{enumTab createPartTab[y;x]}[partkey;]each range;
 
 .[upsert;] each flip (paths;eTabs);
 if [(system "p") in .rxds.qio_ports;
    @[system;("l ",.rxds.IMDB);{-1"Failed to load : ",x,":",y;exit 1}[.rxds.IMDB]];
    send_to_ports["system \"l .\""];
    @[.Q.bv;[`];{show "bv [`] failed";.Q.bv[]}];
    send_to_ports["@[.Q.bv;[`];{show \"bv [`] failed\";.Q.bv[]}]"]
 ]

 }

getPartRange:{""sv (string"exec range from select range:raze(min ",x,";max ",x,") by part:`int$nop xbar ",x," from ",y)}

getPartKey:{""sv (string"exec part from select range:raze(min ",x,";max ",x,") by part:`int$nop xbar ",x," from ",y)}

createPartTab:{
 /{t:""sv (string"select from ",$[1<max count each .rxds.D;[$[98h~type .rxds.D;`.rxds.D;"flip .rxds.D"]];"enlist .rxds.D"]," where ",y," within (",x[0],";",x[1],")");value t}[x;y]
 {t:""sv (string"select from .rxds.D where ",y," within (",x[0],";",x[1],")");value t}[x;y]
 }

splay_append:{[tn;recs] 
 -1"rt=",.rxds.IMDB;
 exists:@[value;tn;`no];
 pk:(exec pk from meta_table where tab like tn)[0];
 $[exists~`no;
  [
    -1"EnteringNew=",tn;
    /-hsym[`$.rxds.IMDB,"/",tn,"/"] upsert .Q.en[hsym`$.rxds.IMDB;] recs;
    hsym[`$.rxds.IMDB,"/",tn,"/"] upsert .Q.en[DBPATH;] recs;
    @[system;("l ",.rxds.IMDB,"/",tn);{-1"Failed to load : ",tn;exit 1}]
  ];
  [
    -1"Enteringexists=",tn;
    /-r:select from recs where not ([]vpids;simid;armid) in select vpids,simid,armid from exists;
    r:splay_upsert[recs;exists;pk];
    if[0<count r;
     [hsym[`$.rxds.IMDB,"/",tn,"/"] upsert .Q.en[DBPATH;] r;
     @[system;("l ",.rxds.IMDB,"/",tn);{-1"Failed to load : ",tn;exit 1}]]
    ]]]
  
 }

splay_upsert:{z:$[0>type z;enlist z;z];?[x;enlist(not;(in;(flip;(lsq;enlist z;enlist,z));(?;y;();0b;z!z)));0b;()]}

check_struct:{
 -1"Checking Struct... ",y;
 s:(raze exec col from meta_table where tab like y)~(raze exec c from meta enlist x);
 if[0b~s;-1"Columns Mismatch";-1"Columns Match"];
 s
 }

check_structV1:{(raze exec col from meta_table where tab like y)~(raze exec c from meta enlist x)}

init_setup:{
 tab:`$x; 
 /-rt:system("pwd");
 st:(exec stor from meta_table where tab like x)[0];
 t:{flip[(raze exec col from meta_table where tab like x)!((exec lower typ from meta_table where tab like x)[0])$\:()]}string tab;
 t,t[`stamp]:.z.Z;

 if[st~`memory;
  [-1"EnteringMemory=",string st;
   k:{raze exec pk from meta_table where tab=x}tab;
   tab set t;
   (k) xkey tab;
   -1"Created=",string tab]];

 if[st~`splayed;
   [-1"EnteringSplayed=",string st;
    hsym[`$.rxds.IMDB,"/",x,"/"] set .Q.en[hsym`$.rxds.IMDB;] t;
    /- hsym[`$"/Users/viswakr/q","/",x,"/"] upsert .Q.en[hsym`$"/Users/viswakr/q";] t;
    -1"CreatedSplayed=",string tab]]

 }

createPartPath:{
 /- x -> partition value
 /- y -> table  

 .Q.dd[hsym .rxds.qio_segment;(x;y;`)]
 }

enumTab:{
 /- x -> table
 .Q.en[DBPATH;x]
 } 

mat_setValue:{[p_namespace;p_table;p_key;p_value]
 t:tab[p_namespace;p_table];
 @[value;t;create_table];
 t1:`$t;
 t1 upsert (`tkey`tvalue)!(p_key;p_value)
 }
 
mat_getValue:{[p_namespace;p_table;p_key]
 t:tab[p_namespace;p_table];
 t1:`$t;
 exec tvalue from t1 where tkey=p_key
 }

mat_reduce:{[p_namespace;p_table;reduce_func]
 t:tab[p_namespace;p_table];
 t1:`$t;
 l:exec tvalue from t1;
 r:parse string reduce_func;
 @[r;l;{"Error with reduce function : ",x}]
 }

mat_gather:{[p_namespace;p_table]
 t:tab[p_namespace;p_table];
 t1:`$t;
 exec tkey,tvalue from t1
 }

mat_save:{[p_namespace;p_table]
 t:tab[p_namespace;p_table];
 typ:(trim exec typ from  meta_table where tab like t)[0];
 if[""~typ;:`$"Cannot save, Attribute values in struct has to be a row vector"];
 t1:`$t;
 cd[p_namespace;t1];
 /-- @[system;("l ",t);{-1"Failed to load : ",x,":",y;exit 1}[t]]
 send_to_ports[("system \"l ",t,"\"")]; 
 /--send_to_ports["system \"l meta_table\""]
 `$"Table saved"
 }

mat_load:{[p_namespace;p_table]
 t:tab[p_namespace;p_table];
 t1:`$t;
 t:ld[p_namespace;t1];
 / - t:ld t1;
 show t;
 t1 set t;
 `loaded
 }

mat_drop:{[p_namespace;p_table]
 t:tab[p_namespace;p_table];
 t1:`$t;
 del[p_namespace;t1];
 `dropped
 }

mat_hetParamReduce:{[p_namespace;p_table;p_param;reduce_func]
 t:tab[p_namespace;p_table];
 t1:`$t;
 l:exec tvalue from t1 where tparam like string p_param;
 r:parse string reduce_func;
 @[r;l;{"Error with reduce function : ",x}]
 }

mat_hetParamGather:{[p_namespace;p_param;p_table]
 t:tab[p_namespace;p_table];
 t1:`$t;
 exec tkey,tvalue from t1 where tparam like string p_param
 }

mat_setHetParamValue:{[p_namespace;p_table;p_key;p_param;p_value]
 t:tab[p_namespace;p_table];
 @[value;t;create_param_table];
 t1:`$t;
 d:enlist [`val]!enlist p_value;
 t1 upsert (`tkey`tparam`tvalue)!(p_key;string p_param;d)
 }

mat_getHetParamValue:{[p_namespace;p_table;p_key;p_param]
 t:tab[p_namespace;p_table];
 t1:`$t;
 exec tvalue from t1
 where tkey=p_key
 ,tparam like string p_param
 }

mat_setHetValue:{[p_namespace;p_table;p_key;p_value]
 t:tab[p_namespace;p_table];
 @[value;t;create_table];
 t1:`$t;
 d:enlist [`val]!enlist p_value;
 t1 upsert (`tkey`tvalue)!(p_key;d)
 }
 
mat_getHetValue:{[p_namespace;p_table;p_key]
 t:tab[p_namespace;p_table];
 t1:`$t;
 exec tvalue from t1
 where tkey=p_key
 }

mat_struct2cols:{[p_namespace;p_table;p_data]
 t:tab[p_namespace;p_table];
 t1:`$t;
 D::p_data;
 @[value;t;create_struct2cols];
 t1 insert ({raze x}exec c from meta D)!({raze exec x from D}each exec c from meta D);
 `logged
 }

create_struct2cols:{
 tab:`$x; 
 t:flip ({raze x}exec c from meta D)!({raze exec x from D}each exec c from meta D) ;
 tab set t;
 show `Created
 }

mat_struct2table:{[p_namespace;p_table;p_key;p_data]
 t:tab[p_namespace;p_table];
 t1:`$t;
 D::p_data;
 K::string p_key;
 @[value;t;create_struct2table];
 t1 upsert select from D;
 `logged
 }

create_struct2table:{
 tab:`$x; 
 t:flip[(value "exec c from meta D")!(ssr[value "exec t from meta D";"[a-z|A-Z]";"*"])$\:()] ;
 tab set t;
 (`$K) xkey tab;
 show `Created
 }

mat_table2table:{[p_namespace;p_table;p_data]
 t:tab[p_namespace;p_table];
 t1:`$t;
 t1 set p_data;
 `logged
 }


tab:{
 t:$[x ~ `symbol$();"" sv(string `,y);"" sv(string `.,x,".",y)];
 t
 }

create_table:{
 .rxds.USED:.z.P;
 tab:`$x; 
 show tab;
 t:([tkey:()] tvalue:());
 tab set t;
 show `Created
 }

create_param_table:{
 tab:`$x; 
 t:([tkey:();tparam:()] tvalue:());
 tab set t;
 show `Created
 }

cd1:{hsym[`$"/"sv 1_"."vs string x]set get x}

ld1:{get hsym[`$"/"sv 1_"."vs string x]}

cd:{
 -1"Saving... ",string y;
 $[x ~ `symbol$();hsym [y]set get y;hsym[`$"/"sv 1_"."vs string y]set get y];
 send_to_ports["system \"l .\""]
 }

ld:{
 -1"Loading... ",string y;
 $[x ~ `symbol$();get hsym [y];get hsym[`$"/"sv 1_"."vs string y]]
 }

del:{
 -1"Dropping...",string y;
 rt:system("pwd");

 st:(exec stor from meta_table where tab like string y)[0];
 if[st~`memory;
   $[x ~ `symbol$();hdel hsym [y];hdel hsym[`$"/"sv 1_"."vs string y]]
 ];

 if[st~`splayed;
  -1"rt=",("rm -fr ",.rxds.IMDB,"/",string y);
  @[system;("rm -fr ",.rxds.IMDB,"/",string y);{-1"Failed to drop : ",y;exit 1}]
 ]

 }

