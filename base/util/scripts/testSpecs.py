#!/usr/bin/env python3

import sys
from subprocess import check_output, CalledProcessError
import shlex

SHOW_INFO_BIN_PROD = '/home/jsoc/cvs/Development/JSOC/bin/linux_avx/show_info'
# SHOW_INFO_BIN_PROD = '/home/jsoc/cvs/Development/JSOC_20190626_141005/_linux_avx/base/util/apps/show_info'
# SHOW_INFO_BIN_DEV = '/home/arta/jsoctrees/JSOC/bin/linux_avx/show_info'
SHOW_INFO_BIN_DEV = '/home/arta/jsoctrees/JSOC/bin/linux_avx/show_info'

COMPARE = True

# show_info tests (which cannot test all DRMS parameter combinations)
SHOW_INFO_SPECS_NOSHADOW = '''\
# no links
# nrecs != 0
hmi.Lw_720s -i n=-725
hmi.Lw_720s -iK n=-725 key=eatme,magnetogram,t_rec
hmi.Lw_720s -i n=-725 key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[]{magnetogram} -i n=-725 key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[?sunum>1148128819?] -i n=-725 key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[2017.2.18/12m] -i n=-725 key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[2019.2.18/1d][?sunum>1137903162?] -i n=-725 key=eatme,t_rec seg=magnetogram,blah
-iK n=-25 key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,1177535037,1177536023,1177533599,1177853705,1177859055,1177854243,1177859303,1178166133,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
## bug in old code reversing recs when n < 0 and sunums in multiple series # -iK n=-25 key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,54243,12342,177859303,11781661235235,33,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
-iK n=-25 key=eatme,magnetogram,t_rec sunum=54243,12342,11781661235235,33,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
# nrecs == 0
hmi.M_720s[] -i key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[]{magnetogram} -i key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[?sunum>1175537273?] -i key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[2017.2.18/12m] -i key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[2019.2.18/30h][?sunum>1137907037?] -iK key=eatme,t_rec seg=magnetogram,blah
hmi.lw_720s[^][1] -i
hmi.lw_720s[][1] -c
hmi.lw_720s[][^] -c
hmi.lw_720s[][3] -c
hmi.lw_720s[][4] -c
# links
# nrecs != 0
hmi.rdvfitsc_fd30 -i n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd30[?sunum>1176337646?] -i n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd30[]{fit.out} -i n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd30[2218]{fit.out} -iK n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd30 -i n=-22 -a seg=fit.out,blah
hmi.rdvfitsc_fd30[2218][?sunum>1176337646?] -i n=-22 -A key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd30[]{fit.out} -i n=-25 -K key=eatme,t_rec seg=fit.out,blah
aia.lev1_euv_12s -iK n=-25 key=AGT1SVZ,t_rec seg=image,blah
# nrecs = 0
######## hmi.rdvfitsc_fd30[] -i key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd30[?sunum>1176337646?] -i key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd30[]{fit.out} -i key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd30[2218]{fit.out} -iK key=eatme,t_rec seg=fit.out,blah
######## hmi.rdvfitsc_fd30[] -i -a seg=fit.out,blah
hmi.rdvfitsc_fd30[2218][?sunum>1176337646?] -i -A key=eatme,magnetogram,t_rec
## hmi.rdvfitsc_fd30[]{fit.out} -i -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd30[2218][^] -i
hmi.rdvfitsc_fd30[^] -c
-iK key=eatme,Apode_f,t_rec seg=fit.out,blah sunum=1178218311,1178218318,1178218314,1178218319,1178218317,1178218323,1178218324,1178218329,1178218330,1178218368
-iK key=eatme,t_rec seg=fit.out,blah sunum=1178218311,1178218318,11788319,1178218317,11782118324,1178218329,1178218330,1178218368
aia.lev1_euv_12s[2015.5.5] -iK key=AGT1SVZ,t_rec seg=image,blah\
'''

SHOW_INFO_SPECS_SHADOW = '''\
# queryStringA - no links
# nrecs != 0
hmi.m_720s -i n=-225
hmi.m_720s -i n=-22 key=eatme,magnetogram,t_rec
hmi.m_720s -i n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -i n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -i n=-225 -a seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -i n=-225 -A key=eatme,t_rec
hmi.m_720s[]{magnetogram} -i n=-225 -A -K key=eatme,t_rec
# nrecs == 0
hmi.m_720s[] -i
# queryStringA - links
# nrecs != 0
hmi.rdvfitsc_fd15 -i n=-25
hmi.rdvfitsc_fd15 -i n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15 -i n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[]{fit.out} -i n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15 -i n=-22 -a seg=fit.out,blah
hmi.rdvfitsc_fd15 -i n=-22 -A key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[]{fit.out} -i n=-25 -K key=eatme,t_rec seg=fit.out,blah
# nrecs == 0
######## hmi.rdvfitsc_fd15[] -i key=eatme,t_rec seg=fit.out,blah
######## hmi.rdvfitsc_fd15[]{fit.out} -i key=eatme,t_rec seg=fit.out,blah
## orig too slow ## hmi.rdvfitsc_fd15[]{fit.out} -i -a seg=fit.out,blah
## orig too slow ## hmi.rdvfitsc_fd15[]{fit.out} -i -A key=eatme,t_rec
## orig too slow ## hmi.rdvfitsc_fd15[] -i -K key=eatme,t_rec seg=fit.out,blah
# queryStringB - no links
# nrecs != 0
hmi.m_720s[?sunum>1148128819?] -i n=-22 key=eatme,magnetogram,t_rec
hmi.m_720s[?sunum>1169888819?] -i n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1148128819?]{magnetogram,duh} -i n=-225 key=eatme,t_rec seg=magnetogram,blah
# nrecs == 0
hmi.m_720s[?sunum>1148128819?] -i key=eatme,magnetogram,t_rec
hmi.m_720s[?sunum>1169888819?] -i key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1148128819?]{magnetogram,duh} -i key=eatme,t_rec seg=magnetogram,blah
# queryStringB - links
# nrecs != 0
hmi.rdvfitsc_fd15[?sunum>1172698819?] -i n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?] -i n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i n=-25 -a seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i n=-25 -K key=eatme,t_rec seg=fit.out,blah
# nrecs == 0
hmi.rdvfitsc_fd15[?sunum>1172698819?] -i key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?] -i key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i -a seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i -A key=eatme,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -i -K key=eatme,t_rec seg=fit.out,blah
# queryStringC - no links
# nrecs != 0
hmi.m_720s[2015.6.7/2h] -i n=-22 key=eatme,magnetogram,t_rec
hmi.m_720s[2015.6.7/2h] -i n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2015.6.7/2h]{magnetogram,duh} -i n=-225 key=eatme,t_rec seg=magnetogram,blah
# nrecs == 0
hmi.m_720s[2015.6.7/2h] -i key=eatme,magnetogram,t_rec
hmi.m_720s[2015.6.7/2h] -i key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2015.6.7/2h]{magnetogram,duh} -i key=eatme,t_rec seg=magnetogram,blah
# queryStringC - links
# nrecs != 0
hmi.rdvfitsc_fd15[2214] -i n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[2214] -i n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i n=-25 -a seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214]{fit.out} -i n=-25 -K key=eatme,t_rec seg=fit.out,blah
# nrecs == 0
hmi.rdvfitsc_fd15[2214] -i key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[2214] -i key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i -a seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214]{fit.out} -i -K key=eatme,t_rec seg=fit.out,blah
# queryStringD - no links
# nrecs != 0
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?] -i n=-22 key=eatme,magnetogram,t_rec
hmi.m_720s[2019.06.14/96m][?sunum>1169888819?] -i n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?]{magnetogram,duh} -i n=-225 key=eatme,t_rec seg=magnetogram,blah
# nrecs == 0
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?] -i key=eatme,magnetogram,t_rec
hmi.m_720s[2019.06.14/96m][?sunum>1169888819?] -i key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?]{magnetogram,duh} -i key=eatme,t_rec seg=magnetogram,blah# queryStringD - links
# queryStringD - links
# nrecs != 0
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -i n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -i n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i n=-25 -a seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i n=-25 -K key=eatme,t_rec seg=fit.out,blah
# nrecs == 0
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -i key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -i key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i -a seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -i -K key=eatme,t_rec seg=fit.out,blah
# queryStringFL - no links
hmi.m_720s[$][1] -i
hmi.m_720s[$][] -i
hmi.m_720s[$][] -i -K key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[$][] -i -a seg=magnetogram,blah
hmi.m_720s[$][] -i -A key=eatme,t_rec
# drms_series_nrecords_querystringA - no links
hmi.m_720s -c
hmi.m_720s -c key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -c key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -c -K key=eatme,t_rec seg=magnetogram,blah
# drms_series_nrecords_querystringA - links
hmi.rdvfitsc_fd15 -c key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[]{fit.out} -c -a seg=fit.out,blah
# drms_series_nrecords_querystringB - no links
hmi.m_720s[?sunum>1169888819?] -c n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1148128819?]{magnetogram,duh} -c n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1169888819?] -c key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1148128819?]{magnetogram,duh} -c key=eatme,t_rec seg=magnetogram,blah
# drms_series_nrecords_querystringB - links
hmi.rdvfitsc_fd15[?sunum>1172698819?] -c n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -c n=-25 -a seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -c n=-25 -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?] -c key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -c -A key=eatme,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -c -K key=eatme,t_rec seg=fit.out,blah
# drms_series_nrecords_querystringC - no links
hmi.m_720s[][1] -c
hmi.m_720s[][2] -c key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[][3] -c
hmi.m_720s[][4] -c
hmi.m_720s[][4] -c -K
# drms_series_nrecords_querystringC - links
hmi.rdvfitsc_fd15[2214] -c n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -c n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214]{fit.out} -c n=-25 -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214] -c key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -c key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -c -K key=eatme,t_rec seg=fit.out,blah
# drms_series_nrecords_querystringD - no links
hmi.m_720s[2019.06.14/96m][?sunum>1169888819?] -c n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?]{magnetogram,duh} -i n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?] -c key=eatme,magnetogram,t_rec
# drms_series_nrecords_querystringD - links
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -c n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -c n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -c n=-25 -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -c key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -c key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -c -K key=eatme,t_rec seg=fit.out,blah
# drms_series_nrecords_querystringFL - no links
hmi.m_720s[$][1] -c
hmi.m_720s[^][] -c
hmi.m_720s[$][] -c -K
hmi.m_720s[][^] -c key=eatme,t_rec seg=magnetogram,blah
# drms_series_nrecords_querystringFL - links
hmi.rdvfitsc_fd15[$][1] -c
hmi.rdvfitsc_fd15[][^][] -c
hmi.rdvfitsc_fd15[$][][] -c -K
hmi.rdvfitsc_fd15[][^] -c key=eatme,carrrot,t_rec seg=fit.out,blah
# no links
-iK -a seg=magnetogram,blah sunum=1176906077,1176898812,1176899989,1176900822,1177221874,1177224920,1177218328,1177224615,1177540420
# links
-io key=eatme,magnetogram,t_rec seg=fit.out,blah sunum=1147883547,1147883552,1147883557,1147883558,1147883559,1147883561,1147883563,1147883566,1147883567,1147883572,1147883574,1147883571,1147883614,1147883616\
'''

SHOW_INFO_SPECS_WEIRD = '''\
# this was due to the fact that a new row was added between the production and dev runs of show_info (d'oh!)
hmi.rdvfitsc_fd30[]{fit.out} -i -K key=eatme,t_rec seg=fit.out,blah\
'''

SHOW_INFO_PATHS = '''\
# no links
hmi.m_720s -i n=-22 -P key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -i n=-225 -P key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -i n=-225 -P -a seg=magnetogram,blah
hmi.m_720s[]{magnetogram} -i n=-225 -P -A key=eatme,t_rec
hmi.m_720s[]{magnetogram} -i n=-225 -P -A -K key=eatme,t_rec
-iPK n=-25 key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,1177535037,1177536023,1177533599,1177853705,1177859055,1177854243,1177859303,1178166133,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
-iP key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,54243,12342,177859303,11781661235235,33,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
# links
hmi.rdvfitsc_fd15[$][1] -P
hmi.rdvfitsc_fd15[][^][] -P
hmi.rdvfitsc_fd15[$][][] -K -P
hmi.rdvfitsc_fd15[][^] -P key=eatme,carrrot,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214] -P n=-22 key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -P n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214]{fit.out} -P n=-25 -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -P -K key=eatme,t_rec seg=fit.out,blah
-iP key=eatme,Apode_f,t_rec seg=fit.out,blah sunum=1178218311,1178218318,1178218314,1178218319,1178218317,1178218323,1178218324,1178218329,1178218330,1178218368
-iPK key=eatme,t_rec seg=fit.out,blah sunum=1178218311,1178218318,11788319,1178218317,11782118324,1178218329,1178218330,1178218368\
'''

SHOW_INFO_COUNTRECS = '''\
hmi.m_720s[2015.2.20/48m]
hmi.rdvfitsc_fd15[$][]
hmi.lw_720s[][1]\
'''

SHOW_INFO_DONTFOLLOWLINKS = '''\
hmi.Lw_720s[2019.2.18/1d][?sunum>1137903162?] -iC n=-725 key=eatme,t_rec seg=magnetogram,blah
-iKC n=-25 key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,1177535037,1177536023,1177533599,1177853705,1177859055,1177854243,1177859303,1178166133,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
## old code had a bug in order of output records ##-iKC n=-25 key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,54243,12342,177859303,11781661235235,33,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
hmi.M_720s[] -iC key=eatme,t_rec seg=magnetogram,blah
hmi.Lw_720s[]{magnetogram} -iC key=eatme,t_rec seg=magnetogram,blah
-iKC key=eatme,Apode_f,t_rec seg=fit.out,blah sunum=1178218311,1178218318,1178218314,1178218319,1178218317,1178218323,1178218324,1178218329,1178218330,1178218368
hmi.m_720s[]{magnetogram} -i n=-225 -AC key=eatme,t_rec
hmi.m_720s[]{magnetogram} -i n=-225 -AC -K key=eatme,t_rec
hmi.m_720s[] -iC
hmi.rdvfitsc_fd15 -iC n=-25
hmi.rdvfitsc_fd15 -iC n=-22 key=eatme,magnetogram,t_rec
hmi.m_720s[?sunum>1148128819?]{magnetogram,duh} -iC n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1169888819?] -iC key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[?sunum>1148128819?]{magnetogram,duh} -i key=eatme,t_rec seg=magnetogram,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?] -iC n=-22 key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -iC n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -iC n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[?sunum>1172698819?]{fit.out} -iC key=eatme,t_rec seg=fit.out,blah
hmi.m_720s[2015.6.7/2h] -iC n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2015.6.7/2h]{magnetogram,duh} -iC n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2015.6.7/2h] -iC key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2015.6.7/2h]{magnetogram,duh} -iC key=eatme,t_rec seg=magnetogram,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -iC n=-25 -a seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -iC n=-25 -A key=eatme,t_rec
hmi.rdvfitsc_fd15[2214]{fit.out} -iC n=-25 -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214] -i key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214]{fit.out} -i -AC key=eatme,t_rec
hmi.m_720s[2019.06.14/96m][?sunum>1169888819?] -iC n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?]{magnetogram,duh} -iC n=-225 key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1169888819?] -iC key=eatme,t_rec seg=magnetogram,blah
hmi.m_720s[2019.06.14/96m][?sunum>1148128819?]{magnetogram,duh} -iC key=eatme,t_rec seg=magnetogram,blah# queryStringD - links
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -iC n=-25 key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?]{fit.out} -iC n=-25 -a seg=fit.out,blah
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -iC key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd15[2214][?sunum>1147883536?] -iC key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.m_720s[$][] -iC -a seg=magnetogram,blah
hmi.m_720s[$][] -iC -A key=eatme,t_rec
hmi.rdvfitsc_fd30[2218][?sunum>1176337646?] -i n=-22 -AC key=eatme,magnetogram,t_rec
hmi.rdvfitsc_fd30[]{fit.out} -iC n=-25 -K key=eatme,t_rec seg=fit.out,blah
aia.lev1_euv_12s -iKC n=-25 key=AGT1SVZ,t_rec seg=image,blah
hmi.rdvfitsc_fd30[?sunum>1176337646?] -iC key=eatme,magnetogram,t_rec seg=fit.out,blah
hmi.m_720s[]{magnetogram} -i n=-225 -PC -A key=eatme,t_rec
hmi.m_720s[]{magnetogram} -i n=-225 -PC -A -K key=eatme,t_rec
-iPCK n=-25 key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,1177535037,1177536023,1177533599,1177853705,1177859055,1177854243,1177859303,1178166133,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
-iPC key=eatme,magnetogram,t_rec sunum=1176547403,1177540424,54243,12342,177859303,11781661235235,33,1178161926,1178166445,1178160904,1178458353,1178458876,1178461075,1178457657
hmi.rdvfitsc_fd15[$][1] -PC
hmi.rdvfitsc_fd15[2214]{fit.out} -PC n=-25 -K key=eatme,t_rec seg=fit.out,blah
hmi.rdvfitsc_fd15[][^][] -PC\
'''

allPassed = True

specs1 = [ line for line in SHOW_INFO_SPECS_NOSHADOW.split('\n') if line[0] != '#' ]
specs2 = [ line for line in SHOW_INFO_SPECS_SHADOW.split('\n') if line[0] != '#' ]
specs3 = [ line for line in SHOW_INFO_SPECS_WEIRD.split('\n') if line[0] != '#' ]
specs4 = [ line for line in SHOW_INFO_PATHS.split('\n') if line[0] != '#' ]
specs5 = [ line for line in SHOW_INFO_COUNTRECS.split('\n') if line[0] != '#' ]
specs6 = [ line for line in SHOW_INFO_DONTFOLLOWLINKS.split('\n') if line[0] != '#' ]

allSpecs = []
allSpecs.extend(specs1)
allSpecs.extend(specs2)
allSpecs.extend(specs3)
allSpecs.extend(specs4)
allSpecs.extend(specs5)
allSpecs.extend(specs6)

outProd = None
outDev = None

for spec in allSpecs:
    quotedSpec = ' '.join([ shlex.quote(arg) for arg in spec.split(' ') ])
    passed = False
    errMsg = bytearray(b'')
    print('testing ' + spec + ': ', end='')
    if COMPARE:
        sys.stdout.flush()
    else:
        print('')

    if COMPARE:
        try:
            cmd = SHOW_INFO_BIN_PROD + ' ' + quotedSpec
            out = check_output(cmd, shell=True)
            if out:
                outProd = out.decode()
        except CalledProcessError as exc:
            errMsg.extend(b'[ prod ] failed to run properly ' + cmd.encode())
            if exc.output:
                outProd = exc.output.decode()

    try:
        cmd = SHOW_INFO_BIN_DEV + ' ' + quotedSpec
        out = check_output(cmd, shell=True)
        if out:
            outDev = out.decode()
    except CalledProcessError as exc:
        errMsg.extend(b'[ dev ] failed to run properly' + cmd.encode())
        if exc.output:
            outDev = exc.output.decode()

    # compare output
    if COMPARE:
        if outProd and outDev:
            if outProd == outDev:
                passed = True

        if passed:
            print('passed')
        else:
            print('failed ' + errMsg.decode())
            allPassed = False

        if allPassed:
            print('** PASSED all tests **')
        else:
            print('** FAILED at least one test **')
