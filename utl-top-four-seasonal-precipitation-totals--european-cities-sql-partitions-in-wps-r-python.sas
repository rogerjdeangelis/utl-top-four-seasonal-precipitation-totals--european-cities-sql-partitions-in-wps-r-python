%let pgm=utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python;

Top 4 seasonal precipitation totals for six european cities by season sql partitions in wps r python;

github
https://tinyurl.com/434hbudn
https://github.com/rogerjdeangelis/utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python

stackoverflow
https://tinyurl.com/4d7jy67v
https://stackoverflow.com/questions/77281973/select-top-n-values-including-for-each-column-of-dataframe-and-show-their-rel

SOLUTIONS

   1, wps sql
   2. wps sql array
   3. wps r sql
   4  wps python sql

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input
 CITY$ SPRING SUMMER FALL WINTER;
cards4;
London 10 1 22 0
Paris 3 5 24 12
Rome 6 6 15 4
Madrid 9 4 4 22
Venice 23 30 12 7
Bern 8 12 8 9
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*                  Top 4 seasonal precipitation totals for six european cities by season                                 */
/*                                                                                                                        */
/*        INPUT                      |       STEP 1                 |            OUTPUT                                   */
/*                                   |                              |                                                     */
/* SD1.HAVE total obs=6              |       PIVOT                  |             SELECT TOP 4                            */
/*                                   |                              |                                                     */
/*  CITY   SPRING SUMMER FALL WINTER |   CITY     SEASON    PRECIP  |          CITY     SEASON    PRECIP                  */
/*                                   |                              |                                                     */
/* London    10      1    22     0   |  Paris     FALL        24    |         Paris     FALL        24                    */
/* Paris      3      5    24    12   |  London    FALL        22    |         London    FALL        22                    */
/* Rome       6      6    15     4   |  Rome      FALL        15    |         Rome      FALL        15                    */
/* Madrid     9      4     4    22   |  Venice    FALL        12    |         Venice    FALL        12                    */
/* Venice    23     30    12     7   |  Bern      FALL         8    |                                                     */
/* Bern       8     12     8     9   |  Madrid    FALL         4    |                                                     */
/*                                   |  Venice    SPRING      23    |         Venice    SPRING      23                    */
/*                                   |  London    SPRING      10    |         London    SPRING      10                    */
/*                                   |  Madrid    SPRING       9    |         Madrid    SPRING       9                    */
/*                                   |  Bern      SPRING       8    |         Bern      SPRING       8                    */
/*                                   |  Rome      SPRING       6    |                                                     */
/*                                   |  Paris     SPRING       3    |                                                     */
/*                                   |  Venice    SUMMER      30    |         Venice    SUMMER      30                    */
/*                                   |  Bern      SUMMER      12    |         Bern      SUMMER      12                    */
/*                                   |  Rome      SUMMER       6    |         Rome      SUMMER       6                    */
/*                                   |  Paris     SUMMER       5    |         Paris     SUMMER       5                    */
/*                                   |  Madrid    SUMMER       4    |                                                     */
/*                                   |  London    SUMMER       1    |                                                     */
/*                                   |  Madrid    WINTER      22    |         Madrid    WINTER      22                    */
/*                                   |  Paris     WINTER      12    |         Paris     WINTER      12                    */
/*                                   |  Bern      WINTER       9    |         Bern      WINTER       9                    */
/*                                   |  Venice    WINTER       7    |         Venice    WINTER       7                    */
/*                                   |  Rome      WINTER       4    |                                                     */
/*                                   |  London    WINTER       0    |                                                     */
/*                                   |                              |                                                     */
/**************************************************************************************************************************/

/*                                  _
/ | __      ___ __  ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| / __|/ _` | |
| |  \ V  V /| |_) \__ \ \__ \ (_| | |
|_|   \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
*/

proc datasets lib=sd1 nolist nodetails mt=data mt=view ;delete want havxpo; run;quit;

%utl_submit_wps54x('
options validvarname=any;
libname sd1 "d:/sd1";
libname sdw wpd "d:/sd1";
proc sql;

  create  view  sdw.havXpo as

  select  city, "SPRING" as season, SPRING as precip from sd1.have union corr
  select  city, "SUMMER" as season, SUMMER as precip from sd1.have union corr
  select  city, "FALL"   as season, FALL   as precip from sd1.have union corr
  select  city, "WINTER" as season, WINTER as precip from sd1.have

  order by season, precip descending,  city
  ;

  create
     table sd1.want(drop=delete) as
  select
     *
  from
      (select *, monotonic() as row_number from
      (select *, max(city) as delete from sdw.havxpo group by season ))
  group
     by season
  having
     (row_number - min(row_nu mber)) <=3

;quit;

proc print data=sd1.want;
run;quit;

');


/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*   CITY     SEASON    PRECIP    ROW_NUMBER                                                                              */
/*                                                                                                                        */
/*  Paris     FALL        24           1                                                                                  */
/*  London    FALL        22           2                                                                                  */
/*  Rome      FALL        15           3                                                                                  */
/*  Venice    FALL        12           4                                                                                  */
/*  Venice    SPRING      23           7                                                                                  */
/*  London    SPRING      10           8                                                                                  */
/*  Madrid    SPRING       9           9                                                                                  */
/*  Bern      SPRING       8          10                                                                                  */
/*  Venice    SUMMER      30          13                                                                                  */
/*  Bern      SUMMER      12          14                                                                                  */
/*  Rome      SUMMER       6          15                                                                                  */
/*  Paris     SUMMER       5          16                                                                                  */
/*  Madrid    WINTER      22          19                                                                                  */
/*  Paris     WINTER      12          20                                                                                  */
/*  Bern      WINTER       9          21                                                                                  */
/*  Venice    WINTER       7          22                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                   _
|___ \  __      ___ __  ___   ___  __ _| |   __ _ _ __ _ __ __ _ _   _
  __) | \ \ /\ / / `_ \/ __| / __|/ _` | |  / _` | `__| `__/ _` | | | |
 / __/   \ V  V /| |_) \__ \ \__ \ (_| | | | (_| | |  | | | (_| | |_| |
|_____|   \_/\_/ | .__/|___/ |___/\__, |_|  \__,_|_|  |_|  \__,_|\__, |
                 |_|                 |_|                         |___/
*/
%array(_se,values=%utl_varlist(sd1.have,drop=city));

%put &=_se2; /* _SE2=SUMMER  */
%put &=_sen; /*  _SE2=SUMMER */

proc datasets lib=sd1 nolist nodetails mt=data mt=view ;delete want havxpo; run;quit;

%utl_submit_wps54x("

options validvarname=any;

libname sd1 'd:/sd1';
libname sdw wpd 'd:/sd1';

options sasautos='c:/otowps';
proc sql;

  create  view sdw.havXpo as

  %do_over(_se,phrase=%str(
    select  city, '?' as season, ? as precip from sd1.have),between=union corr)
  order by season, precip descending,  city
  ;
  create
     table sd1.want(drop=delete) as
  select
     *
  from
      (select *, monotonic() as row_number from
      (select *, max(city) as delete from sdw.havxpo group by season ))
  group
     by season
  having
     (row_number - min(row_number)) <=3

;quit;

proc print data=sd1.want;
run;quit;

");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*   CITY     SEASON    PRECIP    ROW_NUMBER                                                                              */
/*                                                                                                                        */
/*  Paris     FALL        24           1                                                                                  */
/*  London    FALL        22           2                                                                                  */
/*  Rome      FALL        15           3                                                                                  */
/*  Venice    FALL        12           4                                                                                  */
/*  Venice    SPRING      23           7                                                                                  */
/*  London    SPRING      10           8                                                                                  */
/*  Madrid    SPRING       9           9                                                                                  */
/*  Bern      SPRING       8          10                                                                                  */
/*  Venice    SUMMER      30          13                                                                                  */
/*  Bern      SUMMER      12          14                                                                                  */
/*  Rome      SUMMER       6          15                                                                                  */
/*  Paris     SUMMER       5          16                                                                                  */
/*  Madrid    WINTER      22          19                                                                                  */
/*  Paris     WINTER      12          20                                                                                  */
/*  Bern      WINTER       9          21                                                                                  */
/*  Venice    WINTER       7          22                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                         _
|___ /  __      ___ __  ___   _ __   ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |    \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                 |_|                        |_|
*/


proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
havxpo<-sqldf("
  select  city, `SPRING` as season, SPRING as precip from have union
  select  city, `SUMMER` as season, SUMMER as precip from have union
  select  city, `FALL`   as season, FALL   as precip from have union
  select  city, `WINTER` as season, WINTER as precip from have
  order by season, precip DESC,  city
");
havxpo;
want<-sqldf("
  select
     *
  from
      (select *, row_number() over (partition by season) as partition from havxpo )

  where
      partition <= 4
   order
     by season, precip DESC,  city
  ");
want;
endsubmit;
import data=sd1.want r=want;
run;quit;

proc print data=sd1.want width=min;
run;quit;

');

libname sd1 "d:/sd1";
proc print data=sd1.want width=min;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* WPS PROC R                                                                                                             */
/*                                                                                                                        */
/*      CITY season precip partition                                                                                      */
/* 1   Paris   FALL     24         1                                                                                      */
/* 2  London   FALL     22         2                                                                                      */
/* 3    Rome   FALL     15         3                                                                                      */
/* 4  Venice   FALL     12         4                                                                                      */
/* 5  Venice SPRING     23         1                                                                                      */
/* 6  London SPRING     10         2                                                                                      */
/* 7  Madrid SPRING      9         3                                                                                      */
/* 8    Bern SPRING      8         4                                                                                      */
/* 9  Venice SUMMER     30         1                                                                                      */
/* 10   Bern SUMMER     12         2                                                                                      */
/* 11   Rome SUMMER      6         3                                                                                      */
/* 12  Paris SUMMER      5         4                                                                                      */
/* 13 Madrid WINTER     22         1                                                                                      */
/* 14  Paris WINTER     12         2                                                                                      */
/* 15   Bern WINTER      9         3                                                                                      */
/* 16 Venice WINTER      7         4                                                                                      */
/*                                                                                                                        */
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs     CITY     SEASON    PRECIP    PARTITION                                                                         */
/*                                                                                                                        */
/*   1    Paris     FALL        24          1                                                                             */
/*   2    London    FALL        22          2                                                                             */
/*   3    Rome      FALL        15          3                                                                             */
/*   4    Venice    FALL        12          4                                                                             */
/*   5    Venice    SPRING      23          1                                                                             */
/*   6    London    SPRING      10          2                                                                             */
/*   7    Madrid    SPRING       9          3                                                                             */
/*   8    Bern      SPRING       8          4                                                                             */
/*   9    Venice    SUMMER      30          1                                                                             */
/*  10    Bern      SUMMER      12          2                                                                             */
/*  11    Rome      SUMMER       6          3                                                                             */
/*  12    Paris     SUMMER       5          4                                                                             */
/*  13    Madrid    WINTER      22          1                                                                             */
/*  14    Paris     WINTER      12          2                                                                             */
/*  15    Bern      WINTER       9          3                                                                             */
/*  16    Venice    WINTER       7          4                                                                             */
/*                                                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                                     _   _                             _
| || |  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_ \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _| \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                 |_|         |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
options validvarname=any lrecl=32756;
libname sd1 "d:/sd1";
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
mysql = lambda q: sqldf(q, globals());
havxpo = pdsql("""
  select  city, `SPRING` as season,  SPRING as precip from have union
  select  city, `SUMMER` as season,  SUMMER as precip from have union
  select  city, `FALL`   as season,  FALL   as precip from have union
  select  city, `WINTER` as season,  WINTER as precip from have
  order by season, precip DESC,  city
""");
want = pdsql("""
  select
     *
  from
      (select *, row_number() over (partition by season) as partition from havxpo )
  where
      partition <= 4
   order
     by season, precip DESC,  city
  """);
want;
print(havxpo);
endsubmit;
import data=sd1.want python=want;
run;quit;

proc print data=sd1.want width=min;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS Python                                                                                                         */
/*                                                                                                                        */
/* The PYTHON Procedure                                                                                                   */
/*                                                                                                                        */
/*         CITY  season  precip                                                                                           */
/* 0   Paris       FALL    24.0                                                                                           */
/* 1   London      FALL    22.0                                                                                           */
/* 2   Rome        FALL    15.0                                                                                           */
/* 3   Venice      FALL    12.0                                                                                           */
/* 4   Bern        FALL     8.0                                                                                           */
/* 5   Madrid      FALL     4.0                                                                                           */
/* 6   Venice    SPRING    23.0                                                                                           */
/* 7   London    SPRING    10.0                                                                                           */
/* 8   Madrid    SPRING     9.0                                                                                           */
/* 9   Bern      SPRING     8.0                                                                                           */
/* 10  Rome      SPRING     6.0                                                                                           */
/* 11  Paris     SPRING     3.0                                                                                           */
/* 12  Venice    SUMMER    30.0                                                                                           */
/* 13  Bern      SUMMER    12.0                                                                                           */
/* 14  Rome      SUMMER     6.0                                                                                           */
/* 15  Paris     SUMMER     5.0                                                                                           */
/* 16  Madrid    SUMMER     4.0                                                                                           */
/* 17  London    SUMMER     1.0                                                                                           */
/* 18  Madrid    WINTER    22.0                                                                                           */
/* 19  Paris     WINTER    12.0                                                                                           */
/* 20  Bern      WINTER     9.0                                                                                           */
/* 21  Venice    WINTER     7.0                                                                                           */
/* 22  Rome      WINTER     4.0                                                                                           */
/* 23  London    WINTER     0.0                                                                                           */
/*                                                                                                                        */
/* WPS                                                                                                                    */
/*                                                                                                                        */
/* Obs     CITY     season    precip    partition                                                                         */
/*                                                                                                                        */
/*   1    Paris     FALL        24          1                                                                             */
/*   2    London    FALL        22          2                                                                             */
/*   3    Rome      FALL        15          3                                                                             */
/*   4    Venice    FALL        12          4                                                                             */
/*   5    Venice    SPRING      23          1                                                                             */
/*   6    London    SPRING      10          2                                                                             */
/*   7    Madrid    SPRING       9          3                                                                             */
/*   8    Bern      SPRING       8          4                                                                             */
/*   9    Venice    SUMMER      30          1                                                                             */
/*  10    Bern      SUMMER      12          2                                                                             */
/*  11    Rome      SUMMER       6          3                                                                             */
/*  12    Paris     SUMMER       5          4                                                                             */
/*  13    Madrid    WINTER      22          1                                                                             */
/*  14    Paris     WINTER      12          2                                                                             */
/*  15    Bern      WINTER       9          3                                                                             */
/*  16    Venice    WINTER       7          4                                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*
 _ __ ___ _ __   ___  ___
| `__/ _ \ `_ \ / _ \/ __|
| | |  __/ |_) | (_) \__ \
|_|  \___| .__/ \___/|___/
         |_|
*/

REPO
---------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
