# Pareto view

Per regime, each solution's mean on two objectives (both lower is better), over the seeds valid for every solution listed in that table, so all points rest on the same workloads. **Bold** solutions are not dominated by any other solution on that pair. A solution with invalid seeds is marked with its count; one with no valid seed, or no value for a metric, is excluded from the table. When no seed is valid for every solution the points rest on different seeds, so they are listed without a front.

## az-partition

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 7,527,929,675 | 258 |
| az-hier | 600,627,012 | 103 |
| bimodal | 696,489,969 | 103 |
| direct-ack | 1,180,535,615 | 103 |
| flat-once | 1,993,417,809 | 124 |
| gossipsub | 4,233,157,368 | 105 |
| **imp-az-hier** | 373,865,122 | 114 |
| **imp-az-hier-nopk** | 582,003,622 | 103 |
| imp-bimodal | 439,108,802 | 114 |
| imp-bimodal-nopk | 648,788,206 | 103 |
| imp-direct-ack | 545,470,431 | 114 |
| imp-direct-ack-nopk | 848,893,795 | 103 |
| imp-flat-once | 1,912,482,423 | 129 |
| imp-flat-once-nopk | 2,047,185,936 | 115 |
| imp-gossipsub | 2,478,306,344 | 114 |
| imp-gossipsub-nopk | 4,250,332,666 | 107 |
| imp-main | 2,450,345,137 | 119 |
| imp-main-nopk | 2,673,767,840 | 110 |
| imp-owner-agg | 586,159,588 | 365 |
| imp-plumtree | 747,205,489 | 122 |
| imp-plumtree-nopk | 751,277,602 | 122 |
| **imp-scuttlebutt** | 297,445,495 | 223 |
| imp-scuttlebutt-nopk | 396,495,420 | 136 |
| imp-snow | 499,005,559 | 128 |
| imp-snow-nopk | 860,345,511 | 125 |
| imp-thicket | 998,070,104 | 129 |
| imp-thicket-nopk | 1,145,854,988 | 129 |
| **local-only** | 0 | 882 |
| owner-agg | 1,825,928,866 | 370 |
| plumtree | 793,578,145 | 148 |
| scuttlebutt | 472,087,109 | 136 |
| snow | 1,037,963,871 | 190 |
| thicket | 1,532,914,611 | 240 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 7,527,929,675 | 333 |
| az-hier | 600,627,012 | 105 |
| bimodal | 696,489,969 | 105 |
| direct-ack | 1,180,535,615 | 105 |
| flat-once | 1,993,417,809 | 134 |
| gossipsub | 4,233,157,368 | 107 |
| **imp-az-hier** | 373,865,122 | 121 |
| **imp-az-hier-nopk** | 582,003,622 | 105 |
| imp-bimodal | 439,108,802 | 121 |
| imp-bimodal-nopk | 648,788,206 | 105 |
| imp-direct-ack | 545,470,431 | 121 |
| imp-direct-ack-nopk | 848,893,795 | 105 |
| imp-flat-once | 1,912,482,423 | 145 |
| imp-flat-once-nopk | 2,047,185,936 | 123 |
| imp-gossipsub | 2,478,306,344 | 122 |
| imp-gossipsub-nopk | 4,250,332,666 | 110 |
| imp-main | 2,450,345,137 | 127 |
| imp-main-nopk | 2,673,767,840 | 114 |
| imp-owner-agg | 586,159,588 | 315 |
| imp-plumtree | 747,205,489 | 130 |
| imp-plumtree-nopk | 751,277,602 | 132 |
| **imp-scuttlebutt** | 297,445,495 | 253 |
| imp-scuttlebutt-nopk | 396,495,420 | 153 |
| imp-snow | 499,005,559 | 147 |
| imp-snow-nopk | 860,345,511 | 140 |
| imp-thicket | 998,070,104 | 149 |
| imp-thicket-nopk | 1,145,854,988 | 145 |
| **local-only** | 0 | 840 |
| owner-agg | 1,825,928,866 | 320 |
| plumtree | 793,578,145 | 184 |
| scuttlebutt | 472,087,109 | 152 |
| snow | 1,037,963,871 | 250 |
| thicket | 1,532,914,611 | 281 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 7,527,929,675 | 9,970 |
| az-hier | 600,627,012 | 9,969 |
| bimodal | 696,489,969 | 9,972 |
| direct-ack | 1,180,535,615 | 9,968 |
| flat-once | 1,993,417,809 | 9,976 |
| gossipsub | 4,233,157,368 | 9,975 |
| imp-az-hier | 373,865,122 | 9,969 |
| **imp-az-hier-nopk** | 582,003,622 | 9,968 |
| imp-bimodal | 439,108,802 | 9,973 |
| imp-bimodal-nopk | 648,788,206 | 9,972 |
| imp-direct-ack | 545,470,431 | 9,969 |
| imp-direct-ack-nopk | 848,893,795 | 9,968 |
| imp-flat-once | 1,912,482,423 | 9,971 |
| imp-flat-once-nopk | 2,047,185,936 | 9,970 |
| imp-gossipsub | 2,478,306,344 | 9,969 |
| imp-gossipsub-nopk | 4,250,332,666 | 9,969 |
| imp-main | 2,450,345,137 | 9,969 |
| imp-main-nopk | 2,673,767,840 | 9,969 |
| imp-owner-agg | 586,159,588 | 9,971 |
| imp-plumtree | 747,205,489 | 9,970 |
| imp-plumtree-nopk | 751,277,602 | 9,970 |
| **imp-scuttlebutt** | 297,445,495 | 9,969 |
| imp-scuttlebutt-nopk | 396,495,420 | 9,970 |
| imp-snow | 499,005,559 | 9,970 |
| imp-snow-nopk | 860,345,511 | 9,970 |
| imp-thicket | 998,070,104 | 9,970 |
| imp-thicket-nopk | 1,145,854,988 | 9,970 |
| **local-only** | 0 | 9,990 |
| owner-agg | 1,825,928,866 | 9,972 |
| plumtree | 793,578,145 | 9,975 |
| scuttlebutt | 472,087,109 | 9,970 |
| snow | 1,037,963,871 | 9,977 |
| thicket | 1,532,914,611 | 9,976 |

## churn-rollout

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 7,512,905,767 | 210 |
| az-hier | 748,028,857 | 13.7 |
| bimodal | 750,322,860 | 59 |
| direct-ack | 711,105,912 | 18.7 |
| flat-once | 2,190,523,856 | 87.7 |
| gossipsub | 4,820,251,145 | 38.3 |
| **imp-az-hier** | 522,060,021 | 13.3 |
| imp-az-hier-nopk | 772,987,487 | 5 |
| **imp-bimodal** | 475,961,042 | 14.3 |
| imp-bimodal-nopk | 687,298,371 | 4.67 |
| **imp-direct-ack** | 429,815,407 | 25 |
| imp-direct-ack-nopk | 700,757,160 | 16.3 |
| imp-flat-once | 2,314,589,084 | 30.7 |
| imp-flat-once-nopk | 2,473,453,097 | 13 |
| imp-gossipsub | 2,786,719,458 | 13.7 |
| imp-gossipsub-nopk | 4,653,903,308 | 5 |
| imp-main | 2,886,662,987 | 16 |
| imp-main-nopk | 3,126,627,023 | 10 |
| **imp-owner-agg** | 580,779,000 | 1 |
| imp-plumtree | 876,446,117 | 4.67 |
| imp-plumtree-nopk | 874,840,829 | 4.33 |
| **imp-scuttlebutt** | 360,901,125 | 90.3 |
| imp-scuttlebutt-nopk | 484,493,745 | 28.3 |
| imp-snow | 655,297,681 | 15.7 |
| imp-snow-nopk | 1,091,723,781 | 5.33 |
| imp-thicket | 1,201,944,333 | 19.3 |
| imp-thicket-nopk | 1,356,983,140 | 8.33 |
| **local-only** | 0 | 885 |
| owner-agg | 2,085,878,688 | 10 |
| plumtree | 877,399,724 | 8.67 |
| scuttlebutt | 583,896,953 | 28.7 |
| snow | 1,218,139,762 | 97.7 |
| thicket | 1,852,658,287 | 10 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 7,512,905,767 | 270 |
| az-hier | 748,028,857 | 34 |
| bimodal | 750,322,860 | 52 |
| direct-ack | 711,105,912 | 36 |
| flat-once | 2,190,523,856 | 89.7 |
| gossipsub | 4,820,251,145 | 43.3 |
| **imp-az-hier** | 522,060,021 | 19.3 |
| imp-az-hier-nopk | 772,987,487 | 8 |
| **imp-bimodal** | 475,961,042 | 20.3 |
| imp-bimodal-nopk | 687,298,371 | 11.3 |
| **imp-direct-ack** | 429,815,407 | 41.7 |
| imp-direct-ack-nopk | 700,757,160 | 35 |
| imp-flat-once | 2,314,589,084 | 49.3 |
| imp-flat-once-nopk | 2,473,453,097 | 32 |
| imp-gossipsub | 2,786,719,458 | 20.3 |
| imp-gossipsub-nopk | 4,653,903,308 | 10 |
| imp-main | 2,886,662,987 | 35.7 |
| imp-main-nopk | 3,126,627,023 | 31.7 |
| **imp-owner-agg** | 580,779,000 | 1.67 |
| imp-plumtree | 876,446,117 | 19.3 |
| imp-plumtree-nopk | 874,840,829 | 21 |
| **imp-scuttlebutt** | 360,901,125 | 113 |
| imp-scuttlebutt-nopk | 484,493,745 | 46.3 |
| imp-snow | 655,297,681 | 28.3 |
| imp-snow-nopk | 1,091,723,781 | 14.7 |
| imp-thicket | 1,201,944,333 | 32.3 |
| imp-thicket-nopk | 1,356,983,140 | 23.3 |
| **local-only** | 0 | 840 |
| owner-agg | 2,085,878,688 | 27 |
| plumtree | 877,399,724 | 25 |
| scuttlebutt | 583,896,953 | 45.7 |
| snow | 1,218,139,762 | 105 |
| thicket | 1,852,658,287 | 31.7 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 7,512,905,767 | 6,895 |
| az-hier | 748,028,857 | 1,922 |
| bimodal | 750,322,860 | 4,100 |
| direct-ack | 711,105,912 | 2,181 |
| flat-once | 2,190,523,856 | 9,902 |
| **gossipsub** | 4,820,251,145 | 683 |
| imp-az-hier | 522,060,021 | 1,841 |
| imp-az-hier-nopk | 772,987,487 | 1,668 |
| **imp-bimodal** | 475,961,042 | 1,607 |
| imp-bimodal-nopk | 687,298,371 | 1,466 |
| **imp-direct-ack** | 429,815,407 | 2,100 |
| imp-direct-ack-nopk | 700,757,160 | 1,958 |
| imp-flat-once | 2,314,589,084 | 1,150 |
| imp-flat-once-nopk | 2,473,453,097 | 1,407 |
| **imp-gossipsub** | 2,786,719,458 | 784 |
| **imp-gossipsub-nopk** | 4,653,903,308 | 762 |
| imp-main | 2,886,662,987 | 2,540 |
| imp-main-nopk | 3,126,627,023 | 3,640 |
| imp-owner-agg | 580,779,000 | 1,673 |
| imp-plumtree | 876,446,117 | 3,101 |
| imp-plumtree-nopk | 874,840,829 | 3,167 |
| **imp-scuttlebutt** | 360,901,125 | 2,726 |
| **imp-scuttlebutt-nopk** | 484,493,745 | 1,109 |
| imp-snow | 655,297,681 | 2,002 |
| imp-snow-nopk | 1,091,723,781 | 1,686 |
| imp-thicket | 1,201,944,333 | 2,034 |
| imp-thicket-nopk | 1,356,983,140 | 2,004 |
| **local-only** | 0 | 9,990 |
| owner-agg | 2,085,878,688 | 4,784 |
| plumtree | 877,399,724 | 9,814 |
| scuttlebutt | 583,896,953 | 1,118 |
| snow | 1,218,139,762 | 9,938 |
| thicket | 1,852,658,287 | 9,746 |

## hot-key

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 15,183,189,181 | 269 |
| az-hier | 2,911,658,227 | 4 |
| bimodal | 2,977,436,056 | 4 |
| direct-ack | 2,940,044,995 | 4 |
| flat-once | 11,957,266,339 | 8 |
| gossipsub | 29,682,347,332 | 4 |
| **imp-az-hier** | 1,627,231,511 | 16 |
| imp-az-hier-nopk | 2,833,766,237 | 4 |
| imp-bimodal | 1,768,803,017 | 16 |
| imp-bimodal-nopk | 2,816,669,481 | 4 |
| imp-direct-ack | 1,640,865,107 | 16 |
| imp-direct-ack-nopk | 2,940,100,032 | 4 |
| imp-flat-once | 11,365,487,409 | 27.3 |
| imp-flat-once-nopk | 12,173,917,591 | 8 |
| imp-gossipsub | 14,315,431,074 | 16 |
| imp-gossipsub-nopk | 29,666,118,290 | 4 |
| imp-main | 12,295,001,597 | 17 |
| imp-main-nopk | 13,639,024,668 | 5 |
| **imp-owner-agg** | 2,284,175,319 | 1 |
| imp-plumtree | 2,886,701,465 | 2 |
| imp-plumtree-nopk | 2,886,163,055 | 2 |
| **imp-scuttlebutt** | 1,499,735,753 | 86.7 |
| imp-scuttlebutt-nopk | 1,989,867,037 | 23.7 |
| imp-snow | 2,217,148,339 | 16 |
| imp-snow-nopk | 4,383,839,205 | 4 |
| imp-thicket | 3,806,365,200 | 16.7 |
| imp-thicket-nopk | 4,385,013,073 | 4.33 |
| **local-only** | 0 | 536 |
| owner-agg | 10,252,734,343 | 10 |
| plumtree | 2,613,374,265 | 4 |
| scuttlebutt | 2,303,399,414 | 23.7 |
| snow | 4,641,973,506 | 4 |
| thicket | 6,196,479,515 | 4 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 15,183,189,181 | 300 |
| az-hier | 2,911,658,227 | 7.67 |
| bimodal | 2,977,436,056 | 7 |
| direct-ack | 2,940,044,995 | 8 |
| flat-once | 11,957,266,339 | 10.7 |
| gossipsub | 29,682,347,332 | 7.67 |
| **imp-az-hier** | 1,627,231,511 | 20 |
| imp-az-hier-nopk | 2,833,766,237 | 7.67 |
| **imp-bimodal** | 1,768,803,017 | 19.3 |
| imp-bimodal-nopk | 2,816,669,481 | 6.67 |
| imp-direct-ack | 1,640,865,107 | 20 |
| imp-direct-ack-nopk | 2,940,100,032 | 8 |
| imp-flat-once | 11,365,487,409 | 33 |
| imp-flat-once-nopk | 12,173,917,591 | 10 |
| imp-gossipsub | 14,315,431,074 | 20.3 |
| imp-gossipsub-nopk | 29,666,118,290 | 7.67 |
| imp-main | 12,295,001,597 | 22 |
| imp-main-nopk | 13,639,024,668 | 6.67 |
| **imp-owner-agg** | 2,284,175,319 | 1.67 |
| imp-plumtree | 2,886,701,465 | 4 |
| imp-plumtree-nopk | 2,886,163,055 | 2.67 |
| **imp-scuttlebutt** | 1,499,735,753 | 88.3 |
| imp-scuttlebutt-nopk | 1,989,867,037 | 28.7 |
| imp-snow | 2,217,148,339 | 20.3 |
| imp-snow-nopk | 4,383,839,205 | 7.67 |
| imp-thicket | 3,806,365,200 | 20.7 |
| imp-thicket-nopk | 4,385,013,073 | 8 |
| **local-only** | 0 | 471 |
| owner-agg | 10,252,734,343 | 13.3 |
| plumtree | 2,613,374,265 | 8.33 |
| scuttlebutt | 2,303,399,414 | 28 |
| snow | 4,641,973,506 | 7.33 |
| thicket | 6,196,479,515 | 8 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 15,183,189,181 | 9,556 |
| az-hier | 2,911,658,227 | 217 |
| bimodal | 2,977,436,056 | 218 |
| direct-ack | 2,940,044,995 | 217 |
| flat-once | 11,957,266,339 | 9,053 |
| gossipsub | 29,682,347,332 | 218 |
| **imp-az-hier** | 1,627,231,511 | 482 |
| imp-az-hier-nopk | 2,833,766,237 | 217 |
| imp-bimodal | 1,768,803,017 | 481 |
| imp-bimodal-nopk | 2,816,669,481 | 218 |
| **imp-direct-ack** | 1,640,865,107 | 481 |
| imp-direct-ack-nopk | 2,940,100,032 | 217 |
| imp-flat-once | 11,365,487,409 | 1,018 |
| imp-flat-once-nopk | 12,173,917,591 | 674 |
| imp-gossipsub | 14,315,431,074 | 483 |
| imp-gossipsub-nopk | 29,666,118,290 | 217 |
| imp-main | 12,295,001,597 | 516 |
| imp-main-nopk | 13,639,024,668 | 299 |
| **imp-owner-agg** | 2,284,175,319 | 172 |
| imp-plumtree | 2,886,701,465 | 35.9 |
| **imp-plumtree-nopk** | 2,886,163,055 | 35.6 |
| **imp-scuttlebutt** | 1,499,735,753 | 2,786 |
| imp-scuttlebutt-nopk | 1,989,867,037 | 952 |
| imp-snow | 2,217,148,339 | 484 |
| imp-snow-nopk | 4,383,839,205 | 218 |
| imp-thicket | 3,806,365,200 | 500 |
| imp-thicket-nopk | 4,385,013,073 | 233 |
| **local-only** | 0 | 9,990 |
| owner-agg | 10,252,734,343 | 179 |
| plumtree | 2,613,374,265 | 237 |
| scuttlebutt | 2,303,399,414 | 958 |
| snow | 4,641,973,506 | 224 |
| thicket | 6,196,479,515 | 284 |

## idle

bytes_total vs stale_limited_p99: metric not applicable in this regime (no solution has a value for both).

Excluded, no value for bytes_total or stale_limited_p99: main, az-hier, bimodal, direct-ack, flat-once, gossipsub, imp-az-hier, imp-az-hier-nopk, imp-bimodal, imp-bimodal-nopk, imp-direct-ack, imp-direct-ack-nopk, imp-flat-once, imp-flat-once-nopk, imp-gossipsub, imp-gossipsub-nopk, imp-main, imp-main-nopk, imp-owner-agg, imp-plumtree, imp-plumtree-nopk, imp-scuttlebutt, imp-scuttlebutt-nopk, imp-snow, imp-snow-nopk, imp-thicket, imp-thicket-nopk, local-only, owner-agg, plumtree, scuttlebutt, snow, thicket.

bytes_total vs over_max_pct: metric not applicable in this regime (no solution has a value for both).

Excluded, no value for bytes_total or over_max_pct: main, az-hier, bimodal, direct-ack, flat-once, gossipsub, imp-az-hier, imp-az-hier-nopk, imp-bimodal, imp-bimodal-nopk, imp-direct-ack, imp-direct-ack-nopk, imp-flat-once, imp-flat-once-nopk, imp-gossipsub, imp-gossipsub-nopk, imp-main, imp-main-nopk, imp-owner-agg, imp-plumtree, imp-plumtree-nopk, imp-scuttlebutt, imp-scuttlebutt-nopk, imp-snow, imp-snow-nopk, imp-thicket, imp-thicket-nopk, local-only, owner-agg, plumtree, scuttlebutt, snow, thicket.

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 53,347,877 | 1,146 |
| az-hier | 1,397,916 | 499 |
| bimodal | 7,811,416 | 499 |
| direct-ack | 1,441,547 | 499 |
| flat-once | 11,576,314 | 1,279 |
| gossipsub | 15,687,235 | 500 |
| imp-az-hier | 1,397,955 | 499 |
| imp-az-hier-nopk | 1,397,955 | 499 |
| imp-bimodal | 5,410,658 | 499 |
| imp-bimodal-nopk | 5,410,658 | 499 |
| imp-direct-ack | 1,441,547 | 499 |
| imp-direct-ack-nopk | 1,441,547 | 499 |
| imp-flat-once | 13,603,392 | 1,071 |
| imp-flat-once-nopk | 13,603,392 | 1,071 |
| imp-gossipsub | 15,933,573 | 500 |
| imp-gossipsub-nopk | 15,933,573 | 500 |
| imp-main | 32,933,913 | 543 |
| imp-main-nopk | 32,933,913 | 543 |
| **imp-owner-agg** | 648,128 | 189 |
| imp-plumtree | 16,668,609 | 513 |
| imp-plumtree-nopk | 16,668,609 | 513 |
| imp-scuttlebutt | 6,163,507 | 2,570 |
| imp-scuttlebutt-nopk | 6,163,507 | 2,570 |
| imp-snow | 8,927,583 | 500 |
| imp-snow-nopk | 8,927,583 | 500 |
| imp-thicket | 12,105,528 | 503 |
| imp-thicket-nopk | 12,105,528 | 503 |
| **local-only** | 0 | 9,962 |
| **owner-agg** | 1,604,186 | 189 |
| plumtree | 20,794,389 | 515 |
| scuttlebutt | 8,500,708 | 2,570 |
| snow | 8,202,385 | 500 |
| thicket | 18,921,869 | 503 |

## long-window

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 26,663,945,377 | 10 |
| az-hier | 541,147,125 | 2 |
| bimodal | 976,747,488 | 2 |
| direct-ack | 622,794,861 | 2 |
| flat-once | 3,596,678,102 | 3 |
| gossipsub | 3,223,033,365 | 2 |
| imp-az-hier | 537,015,988 | 2 |
| imp-az-hier-nopk | 541,174,274 | 2 |
| imp-bimodal | 982,122,796 | 2 |
| imp-bimodal-nopk | 985,398,916 | 2 |
| imp-direct-ack | 616,859,185 | 2 |
| imp-direct-ack-nopk | 622,796,085 | 2 |
| imp-flat-once | 2,387,392,387 | 3 |
| imp-flat-once-nopk | 2,387,263,280 | 3 |
| imp-gossipsub | 3,161,415,408 | 2 |
| imp-gossipsub-nopk | 3,234,636,922 | 2 |
| imp-main | 4,545,196,700 | 2 |
| imp-main-nopk | 4,581,393,829 | 2 |
| **imp-owner-agg** | 349,785,960 | 0 |
| imp-plumtree | 1,093,159,870 | 1 |
| imp-plumtree-nopk | 1,102,692,915 | 1 |
| imp-scuttlebutt | 532,038,939 | 6 |
| imp-scuttlebutt-nopk | 543,830,187 | 5.67 |
| imp-snow | 863,774,872 | 2 |
| imp-snow-nopk | 871,495,535 | 2 |
| imp-thicket | 965,758,465 | 2 |
| imp-thicket-nopk | 965,674,351 | 2 |
| **local-only** | 0 | 5,930 |
| owner-agg | 150,595,849,335 | 2 |
| plumtree | 3,059,214,983 | 2 |
| scuttlebutt | 785,786,502 | 5.67 |
| snow | 3,086,849,375 | 2 |
| thicket | 3,422,956,665 | 2 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 26,663,945,377 | 7.55 |
| az-hier | 541,147,125 | 0.533 |
| bimodal | 976,747,488 | 0.517 |
| direct-ack | 622,794,861 | 0.517 |
| flat-once | 3,596,678,102 | 1.18 |
| gossipsub | 3,223,033,365 | 0.533 |
| imp-az-hier | 537,015,988 | 0.75 |
| imp-az-hier-nopk | 541,174,274 | 0.533 |
| imp-bimodal | 982,122,796 | 0.733 |
| imp-bimodal-nopk | 985,398,916 | 0.533 |
| imp-direct-ack | 616,859,185 | 0.75 |
| imp-direct-ack-nopk | 622,796,085 | 0.533 |
| imp-flat-once | 2,387,392,387 | 1.32 |
| imp-flat-once-nopk | 2,387,263,280 | 1.15 |
| imp-gossipsub | 3,161,415,408 | 0.767 |
| imp-gossipsub-nopk | 3,234,636,922 | 0.533 |
| imp-main | 4,545,196,700 | 0.783 |
| imp-main-nopk | 4,581,393,829 | 0.517 |
| **imp-owner-agg** | 349,785,960 | 0.0333 |
| imp-plumtree | 1,093,159,870 | 0.15 |
| imp-plumtree-nopk | 1,102,692,915 | 0.15 |
| imp-scuttlebutt | 532,038,939 | 4.28 |
| imp-scuttlebutt-nopk | 543,830,187 | 3.45 |
| imp-snow | 863,774,872 | 0.767 |
| imp-snow-nopk | 871,495,535 | 0.533 |
| imp-thicket | 965,758,465 | 0.783 |
| imp-thicket-nopk | 965,674,351 | 0.55 |
| **local-only** | 0 | 203 |
| owner-agg | 150,595,849,335 | 0.35 |
| plumtree | 3,059,214,983 | 0.533 |
| scuttlebutt | 785,786,502 | 3.45 |
| snow | 3,086,849,375 | 0.533 |
| thicket | 3,422,956,665 | 0.583 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 26,663,945,377 | 6,827 |
| az-hier | 541,147,125 | 494 |
| bimodal | 976,747,488 | 494 |
| direct-ack | 622,794,861 | 494 |
| flat-once | 3,596,678,102 | 5,104 |
| gossipsub | 3,223,033,365 | 495 |
| imp-az-hier | 537,015,988 | 496 |
| imp-az-hier-nopk | 541,174,274 | 494 |
| imp-bimodal | 982,122,796 | 496 |
| imp-bimodal-nopk | 985,398,916 | 494 |
| imp-direct-ack | 616,859,185 | 496 |
| imp-direct-ack-nopk | 622,796,085 | 494 |
| imp-flat-once | 2,387,392,387 | 1,027 |
| imp-flat-once-nopk | 2,387,263,280 | 1,008 |
| imp-gossipsub | 3,161,415,408 | 497 |
| imp-gossipsub-nopk | 3,234,636,922 | 495 |
| imp-main | 4,545,196,700 | 3,896 |
| imp-main-nopk | 4,581,393,829 | 3,684 |
| **imp-owner-agg** | 349,785,960 | 112 |
| imp-plumtree | 1,093,159,870 | 310 |
| imp-plumtree-nopk | 1,102,692,915 | 284 |
| imp-scuttlebutt | 532,038,939 | 2,563 |
| imp-scuttlebutt-nopk | 543,830,187 | 2,479 |
| imp-snow | 863,774,872 | 497 |
| imp-snow-nopk | 871,495,535 | 496 |
| imp-thicket | 965,758,465 | 506 |
| imp-thicket-nopk | 965,674,351 | 503 |
| **local-only** | 0 | 38,614 |
| owner-agg | 150,595,849,335 | 187 |
| plumtree | 3,059,214,983 | 508 |
| scuttlebutt | 785,786,502 | 2,476 |
| snow | 3,086,849,375 | 496 |
| thicket | 3,422,956,665 | 504 |

## loss-20

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 12,964,995,819 | 431 |
| az-hier | 3,523,636,872 | 14 |
| bimodal | 2,986,686,484 | 11.7 |
| direct-ack | 3,304,461,835 | 10 |
| flat-once | 11,021,783,813 | 8.67 |
| gossipsub | 27,365,928,389 | 3.67 |
| imp-az-hier | 3,067,980,948 | 24.3 |
| imp-az-hier-nopk | 4,242,380,215 | 7.33 |
| **imp-bimodal** | 1,914,980,500 | 22 |
| imp-bimodal-nopk | 2,871,367,329 | 11 |
| imp-direct-ack | 1,999,494,437 | 25.3 |
| imp-direct-ack-nopk | 3,368,160,810 | 9.67 |
| imp-flat-once | 10,885,892,828 | 33 |
| imp-flat-once-nopk | 11,380,743,234 | 9 |
| imp-gossipsub | 13,739,447,124 | 14.7 |
| imp-gossipsub-nopk | 27,418,175,842 | 3.67 |
| imp-main | 11,270,036,333 | 16.3 |
| imp-main-nopk | 12,498,197,293 | 4.67 |
| **imp-owner-agg** | 2,869,207,098 | 3 |
| imp-plumtree | 3,391,983,255 | 4 |
| imp-plumtree-nopk | 3,418,636,123 | 4 |
| **imp-scuttlebutt** | 1,737,412,239 | 214 |
| imp-scuttlebutt-nopk | 2,058,303,885 | 53.3 |
| imp-snow | 2,656,040,780 | 24 |
| imp-snow-nopk | 4,805,267,349 | 8 |
| imp-thicket | 4,819,113,368 | 29.7 |
| imp-thicket-nopk | 5,509,091,929 | 12.3 |
| **local-only** | 0 | 894 |
| owner-agg | 7,037,423,503 | 11 |
| plumtree | 3,039,171,515 | 6.67 |
| scuttlebutt | 2,327,981,110 | 53.3 |
| snow | 3,864,139,021 | 48 |
| thicket | 6,683,373,421 | 12.3 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 12,964,995,819 | 473 |
| az-hier | 3,523,636,872 | 23.7 |
| bimodal | 2,986,686,484 | 22 |
| direct-ack | 3,304,461,835 | 19 |
| flat-once | 11,021,783,813 | 12.3 |
| gossipsub | 27,365,928,389 | 5.33 |
| imp-az-hier | 3,067,980,948 | 38.3 |
| imp-az-hier-nopk | 4,242,380,215 | 10.7 |
| **imp-bimodal** | 1,914,980,500 | 37 |
| imp-bimodal-nopk | 2,871,367,329 | 20.7 |
| imp-direct-ack | 1,999,494,437 | 41.3 |
| imp-direct-ack-nopk | 3,368,160,810 | 17 |
| imp-flat-once | 10,885,892,828 | 48.7 |
| imp-flat-once-nopk | 11,380,743,234 | 13.3 |
| imp-gossipsub | 13,739,447,124 | 24.3 |
| imp-gossipsub-nopk | 27,418,175,842 | 5.33 |
| imp-main | 11,270,036,333 | 26.3 |
| imp-main-nopk | 12,498,197,293 | 7.33 |
| **imp-owner-agg** | 2,869,207,098 | 3.33 |
| imp-plumtree | 3,391,983,255 | 6 |
| imp-plumtree-nopk | 3,418,636,123 | 5.33 |
| **imp-scuttlebutt** | 1,737,412,239 | 245 |
| imp-scuttlebutt-nopk | 2,058,303,885 | 73 |
| imp-snow | 2,656,040,780 | 38 |
| imp-snow-nopk | 4,805,267,349 | 12.7 |
| imp-thicket | 4,819,113,368 | 44.7 |
| imp-thicket-nopk | 5,509,091,929 | 20 |
| **local-only** | 0 | 844 |
| owner-agg | 7,037,423,503 | 15.3 |
| plumtree | 3,039,171,515 | 9.33 |
| scuttlebutt | 2,327,981,110 | 70 |
| snow | 3,864,139,021 | 55.3 |
| thicket | 6,683,373,421 | 22.3 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 12,964,995,819 | 9,877 |
| az-hier | 3,523,636,872 | 3,400 |
| bimodal | 2,986,686,484 | 6,992 |
| direct-ack | 3,304,461,835 | 3,063 |
| flat-once | 11,021,783,813 | 9,830 |
| **gossipsub** | 27,365,928,389 | 304 |
| imp-az-hier | 3,067,980,948 | 2,600 |
| imp-az-hier-nopk | 4,242,380,215 | 1,254 |
| **imp-bimodal** | 1,914,980,500 | 5,833 |
| imp-bimodal-nopk | 2,871,367,329 | 5,547 |
| imp-direct-ack | 1,999,494,437 | 6,681 |
| imp-direct-ack-nopk | 3,368,160,810 | 3,061 |
| imp-flat-once | 10,885,892,828 | 1,693 |
| imp-flat-once-nopk | 11,380,743,234 | 1,511 |
| **imp-gossipsub** | 13,739,447,124 | 499 |
| imp-gossipsub-nopk | 27,418,175,842 | 320 |
| imp-main | 11,270,036,333 | 9,682 |
| imp-main-nopk | 12,498,197,293 | 9,658 |
| **imp-owner-agg** | 2,869,207,098 | 1,380 |
| **imp-plumtree** | 3,391,983,255 | 758 |
| **imp-plumtree-nopk** | 3,418,636,123 | 692 |
| **imp-scuttlebutt** | 1,737,412,239 | 6,427 |
| **imp-scuttlebutt-nopk** | 2,058,303,885 | 2,040 |
| imp-snow | 2,656,040,780 | 2,856 |
| imp-snow-nopk | 4,805,267,349 | 2,168 |
| imp-thicket | 4,819,113,368 | 2,981 |
| imp-thicket-nopk | 5,509,091,929 | 3,084 |
| **local-only** | 0 | 9,990 |
| owner-agg | 7,037,423,503 | 9,967 |
| **plumtree** | 3,039,171,515 | 883 |
| **scuttlebutt** | 2,327,981,110 | 2,024 |
| snow | 3,864,139,021 | 9,985 |
| thicket | 6,683,373,421 | 9,959 |

## loss-5

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 12,956,366,561 | 419 |
| az-hier | 2,918,518,851 | 6 |
| bimodal | 2,818,134,681 | 5 |
| direct-ack | 2,867,252,935 | 5 |
| flat-once | 10,957,923,056 | 7.67 |
| gossipsub | 27,248,100,346 | 3.67 |
| imp-az-hier | 1,749,190,699 | 17 |
| imp-az-hier-nopk | 2,986,716,603 | 4.67 |
| **imp-bimodal** | 1,715,458,321 | 16.3 |
| imp-bimodal-nopk | 2,656,250,089 | 4.67 |
| **imp-direct-ack** | 1,637,153,533 | 17.7 |
| imp-direct-ack-nopk | 2,895,701,200 | 5 |
| imp-flat-once | 10,602,443,676 | 29.7 |
| imp-flat-once-nopk | 11,224,185,822 | 7.67 |
| imp-gossipsub | 13,664,604,873 | 15 |
| imp-gossipsub-nopk | 27,467,574,735 | 3.67 |
| imp-main | 11,247,988,107 | 16 |
| imp-main-nopk | 12,485,255,899 | 4.33 |
| **imp-owner-agg** | 2,200,314,106 | 1 |
| imp-plumtree | 2,859,144,264 | 2 |
| imp-plumtree-nopk | 2,869,477,511 | 2 |
| **imp-scuttlebutt** | 1,400,413,397 | 141 |
| imp-scuttlebutt-nopk | 1,853,305,697 | 34 |
| imp-snow | 2,389,126,746 | 16 |
| imp-snow-nopk | 4,516,375,207 | 4 |
| imp-thicket | 3,955,281,712 | 17.3 |
| imp-thicket-nopk | 4,452,882,443 | 4.67 |
| **local-only** | 0 | 894 |
| owner-agg | 6,994,519,277 | 8 |
| plumtree | 2,599,135,405 | 4.67 |
| scuttlebutt | 2,131,664,256 | 33.7 |
| snow | 4,669,566,120 | 9.67 |
| thicket | 6,042,713,129 | 5 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 12,956,366,561 | 456 |
| az-hier | 2,918,518,851 | 10 |
| bimodal | 2,818,134,681 | 7 |
| direct-ack | 2,867,252,935 | 7.67 |
| flat-once | 10,957,923,056 | 10.3 |
| gossipsub | 27,248,100,346 | 5.33 |
| imp-az-hier | 1,749,190,699 | 26.7 |
| imp-az-hier-nopk | 2,986,716,603 | 7 |
| **imp-bimodal** | 1,715,458,321 | 26.3 |
| imp-bimodal-nopk | 2,656,250,089 | 7.67 |
| **imp-direct-ack** | 1,637,153,533 | 29 |
| imp-direct-ack-nopk | 2,895,701,200 | 7.33 |
| imp-flat-once | 10,602,443,676 | 44 |
| imp-flat-once-nopk | 11,224,185,822 | 10.3 |
| imp-gossipsub | 13,664,604,873 | 24.3 |
| imp-gossipsub-nopk | 27,467,574,735 | 5.33 |
| imp-main | 11,247,988,107 | 26 |
| imp-main-nopk | 12,485,255,899 | 6 |
| **imp-owner-agg** | 2,200,314,106 | 1.67 |
| imp-plumtree | 2,859,144,264 | 2.67 |
| imp-plumtree-nopk | 2,869,477,511 | 3 |
| **imp-scuttlebutt** | 1,400,413,397 | 163 |
| imp-scuttlebutt-nopk | 1,853,305,697 | 50.7 |
| imp-snow | 2,389,126,746 | 25 |
| imp-snow-nopk | 4,516,375,207 | 6 |
| imp-thicket | 3,955,281,712 | 26 |
| imp-thicket-nopk | 4,452,882,443 | 6.33 |
| **local-only** | 0 | 844 |
| owner-agg | 6,994,519,277 | 15.7 |
| plumtree | 2,599,135,405 | 5.67 |
| scuttlebutt | 2,131,664,256 | 50.3 |
| snow | 4,669,566,120 | 15.3 |
| thicket | 6,042,713,129 | 7.33 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 12,956,366,561 | 9,746 |
| az-hier | 2,918,518,851 | 1,903 |
| bimodal | 2,818,134,681 | 2,886 |
| direct-ack | 2,867,252,935 | 1,744 |
| flat-once | 10,957,923,056 | 9,505 |
| gossipsub | 27,248,100,346 | 301 |
| **imp-az-hier** | 1,749,190,699 | 1,507 |
| imp-az-hier-nopk | 2,986,716,603 | 772 |
| **imp-bimodal** | 1,715,458,321 | 2,515 |
| imp-bimodal-nopk | 2,656,250,089 | 2,304 |
| **imp-direct-ack** | 1,637,153,533 | 3,219 |
| imp-direct-ack-nopk | 2,895,701,200 | 1,673 |
| imp-flat-once | 10,602,443,676 | 1,141 |
| imp-flat-once-nopk | 11,224,185,822 | 997 |
| imp-gossipsub | 13,664,604,873 | 496 |
| imp-gossipsub-nopk | 27,467,574,735 | 304 |
| imp-main | 11,247,988,107 | 960 |
| imp-main-nopk | 12,485,255,899 | 497 |
| **imp-owner-agg** | 2,200,314,106 | 805 |
| **imp-plumtree** | 2,859,144,264 | 258 |
| imp-plumtree-nopk | 2,869,477,511 | 266 |
| **imp-scuttlebutt** | 1,400,413,397 | 3,782 |
| **imp-scuttlebutt-nopk** | 1,853,305,697 | 1,231 |
| imp-snow | 2,389,126,746 | 1,284 |
| imp-snow-nopk | 4,516,375,207 | 680 |
| imp-thicket | 3,955,281,712 | 1,211 |
| imp-thicket-nopk | 4,452,882,443 | 893 |
| **local-only** | 0 | 9,990 |
| owner-agg | 6,994,519,277 | 9,190 |
| **plumtree** | 2,599,135,405 | 458 |
| scuttlebutt | 2,131,664,256 | 1,234 |
| snow | 4,669,566,120 | 9,981 |
| thicket | 6,042,713,129 | 1,051 |

## loss-burst

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 12,955,625,869 | 417 |
| az-hier | 2,782,306,417 | 6 |
| bimodal | 2,809,427,043 | 5 |
| direct-ack | 2,896,914,601 | 6 |
| flat-once | 10,954,606,842 | 7.67 |
| gossipsub | 27,077,366,587 | 3.67 |
| imp-az-hier | 1,787,956,963 | 17.3 |
| imp-az-hier-nopk | 2,833,125,774 | 5 |
| **imp-bimodal** | 1,714,272,285 | 16.3 |
| imp-bimodal-nopk | 2,650,881,454 | 5 |
| **imp-direct-ack** | 1,702,194,696 | 18 |
| imp-direct-ack-nopk | 2,853,270,548 | 5.67 |
| imp-flat-once | 10,415,894,389 | 29.7 |
| imp-flat-once-nopk | 11,167,232,981 | 7.67 |
| imp-gossipsub | 13,550,127,654 | 15 |
| imp-gossipsub-nopk | 27,048,401,703 | 3.67 |
| imp-main | 11,270,215,014 | 15.3 |
| imp-main-nopk | 12,492,534,715 | 4.33 |
| **imp-owner-agg** | 2,149,269,412 | 6 |
| imp-plumtree | 2,913,496,856 | 2 |
| **imp-plumtree-nopk** | 2,909,288,817 | 2 |
| **imp-scuttlebutt** | 1,382,594,095 | 139 |
| imp-scuttlebutt-nopk | 1,844,453,491 | 34 |
| imp-snow | 2,362,308,665 | 17.3 |
| imp-snow-nopk | 4,460,480,066 | 5 |
| imp-thicket | 4,057,538,482 | 18.7 |
| imp-thicket-nopk | 4,551,420,055 | 6.33 |
| **local-only** | 0 | 894 |
| owner-agg | 7,010,642,999 | 9 |
| **plumtree** | 2,563,425,504 | 5 |
| scuttlebutt | 2,120,456,561 | 33.3 |
| snow | 4,627,559,297 | 10.7 |
| thicket | 5,942,539,229 | 7 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 12,955,625,869 | 457 |
| az-hier | 2,782,306,417 | 9.33 |
| bimodal | 2,809,427,043 | 8 |
| direct-ack | 2,896,914,601 | 9 |
| flat-once | 10,954,606,842 | 11.3 |
| gossipsub | 27,077,366,587 | 5.33 |
| imp-az-hier | 1,787,956,963 | 28 |
| imp-az-hier-nopk | 2,833,125,774 | 8 |
| **imp-bimodal** | 1,714,272,285 | 27 |
| imp-bimodal-nopk | 2,650,881,454 | 7.33 |
| **imp-direct-ack** | 1,702,194,696 | 29 |
| imp-direct-ack-nopk | 2,853,270,548 | 9.33 |
| imp-flat-once | 10,415,894,389 | 43.7 |
| imp-flat-once-nopk | 11,167,232,981 | 9.33 |
| imp-gossipsub | 13,550,127,654 | 24.3 |
| imp-gossipsub-nopk | 27,048,401,703 | 5.33 |
| imp-main | 11,270,215,014 | 25.3 |
| imp-main-nopk | 12,492,534,715 | 6.33 |
| **imp-owner-agg** | 2,149,269,412 | 4.67 |
| **imp-plumtree** | 2,913,496,856 | 3.33 |
| **imp-plumtree-nopk** | 2,909,288,817 | 3.67 |
| **imp-scuttlebutt** | 1,382,594,095 | 163 |
| imp-scuttlebutt-nopk | 1,844,453,491 | 49.7 |
| imp-snow | 2,362,308,665 | 28.3 |
| imp-snow-nopk | 4,460,480,066 | 8 |
| imp-thicket | 4,057,538,482 | 29.7 |
| imp-thicket-nopk | 4,551,420,055 | 10 |
| **local-only** | 0 | 844 |
| owner-agg | 7,010,642,999 | 13.7 |
| plumtree | 2,563,425,504 | 7.33 |
| scuttlebutt | 2,120,456,561 | 48 |
| snow | 4,627,559,297 | 16 |
| thicket | 5,942,539,229 | 8.67 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 12,955,625,869 | 9,743 |
| az-hier | 2,782,306,417 | 2,861 |
| bimodal | 2,809,427,043 | 2,961 |
| direct-ack | 2,896,914,601 | 4,841 |
| flat-once | 10,954,606,842 | 9,484 |
| gossipsub | 27,077,366,587 | 301 |
| imp-az-hier | 1,787,956,963 | 4,810 |
| imp-az-hier-nopk | 2,833,125,774 | 2,495 |
| **imp-bimodal** | 1,714,272,285 | 2,512 |
| imp-bimodal-nopk | 2,650,881,454 | 2,191 |
| imp-direct-ack | 1,702,194,696 | 8,596 |
| imp-direct-ack-nopk | 2,853,270,548 | 4,834 |
| **imp-flat-once** | 10,415,894,389 | 1,149 |
| **imp-flat-once-nopk** | 11,167,232,981 | 962 |
| **imp-gossipsub** | 13,550,127,654 | 496 |
| **imp-gossipsub-nopk** | 27,048,401,703 | 301 |
| imp-main | 11,270,215,014 | 3,360 |
| **imp-main-nopk** | 12,492,534,715 | 713 |
| imp-owner-agg | 2,149,269,412 | 4,648 |
| imp-plumtree | 2,913,496,856 | 2,981 |
| imp-plumtree-nopk | 2,909,288,817 | 3,000 |
| **imp-scuttlebutt** | 1,382,594,095 | 3,651 |
| **imp-scuttlebutt-nopk** | 1,844,453,491 | 1,251 |
| imp-snow | 2,362,308,665 | 5,768 |
| imp-snow-nopk | 4,460,480,066 | 4,745 |
| imp-thicket | 4,057,538,482 | 2,212 |
| imp-thicket-nopk | 4,551,420,055 | 2,215 |
| **local-only** | 0 | 9,990 |
| owner-agg | 7,010,642,999 | 9,350 |
| plumtree | 2,563,425,504 | 9,695 |
| **scuttlebutt** | 2,120,456,561 | 1,224 |
| snow | 4,627,559,297 | 9,981 |
| thicket | 5,942,539,229 | 9,871 |

## low-3

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 9,596,755 | 1 |
| **az-hier** | 246,657 | 1 |
| bimodal | 366,813 | 1 |
| direct-ack | 251,351 | 1 |
| flat-once | 2,704,088 | 1 |
| gossipsub | 418,747 | 1 |
| **imp-az-hier** | 185,411 | 2 |
| **imp-az-hier-nopk** | 246,657 | 1 |
| imp-bimodal | 305,844 | 2 |
| imp-bimodal-nopk | 325,211 | 1 |
| imp-direct-ack | 188,455 | 2 |
| imp-direct-ack-nopk | 251,351 | 1 |
| imp-flat-once | 446,821 | 2 |
| imp-flat-once-nopk | 481,916 | 1 |
| imp-gossipsub | 332,171 | 2 |
| imp-gossipsub-nopk | 418,747 | 1 |
| imp-main | 711,258 | 2 |
| imp-main-nopk | 835,052 | 1 |
| **imp-owner-agg** | 404,955 | 0 |
| imp-plumtree | 322,223 | 2 |
| imp-plumtree-nopk | 368,380 | 1 |
| imp-scuttlebutt | 307,531 | 2.33 |
| imp-scuttlebutt-nopk | 437,339 | 2 |
| imp-snow | 322,683 | 2 |
| imp-snow-nopk | 410,359 | 1 |
| imp-thicket | 680,634 | 2 |
| imp-thicket-nopk | 725,871 | 1 |
| **local-only** | 0 | 39.3 |
| owner-agg | 894,713 | 1 |
| plumtree | 2,748,245 | 1 |
| scuttlebutt | 505,640 | 2 |
| snow | 2,837,763 | 1 |
| thicket | 3,262,814 | 1 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 9,596,755 | 8.33 |
| **az-hier** | 246,657 | 8.33 |
| bimodal | 366,813 | 5 |
| direct-ack | 251,351 | 8.33 |
| flat-once | 2,704,088 | 8.33 |
| gossipsub | 418,747 | 6.67 |
| **imp-az-hier** | 185,411 | 10 |
| **imp-az-hier-nopk** | 246,657 | 8.33 |
| imp-bimodal | 305,844 | 10 |
| **imp-bimodal-nopk** | 325,211 | 5 |
| imp-direct-ack | 188,455 | 10 |
| imp-direct-ack-nopk | 251,351 | 8.33 |
| imp-flat-once | 446,821 | 10 |
| imp-flat-once-nopk | 481,916 | 8.33 |
| imp-gossipsub | 332,171 | 10 |
| imp-gossipsub-nopk | 418,747 | 6.67 |
| imp-main | 711,258 | 10 |
| imp-main-nopk | 835,052 | 8.33 |
| **imp-owner-agg** | 404,955 | 1.67 |
| imp-plumtree | 322,223 | 10 |
| imp-plumtree-nopk | 368,380 | 5 |
| imp-scuttlebutt | 307,531 | 10 |
| imp-scuttlebutt-nopk | 437,339 | 5 |
| imp-snow | 322,683 | 10 |
| imp-snow-nopk | 410,359 | 8.33 |
| imp-thicket | 680,634 | 10 |
| imp-thicket-nopk | 725,871 | 8.33 |
| **local-only** | 0 | 192 |
| owner-agg | 894,713 | 5 |
| plumtree | 2,748,245 | 8.33 |
| scuttlebutt | 505,640 | 5 |
| snow | 2,837,763 | 8.33 |
| thicket | 3,262,814 | 10 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 9,596,755 | 452 |
| az-hier | 246,657 | 452 |
| **bimodal** | 366,813 | 450 |
| direct-ack | 251,351 | 452 |
| flat-once | 2,704,088 | 452 |
| gossipsub | 418,747 | 448 |
| **imp-az-hier** | 185,411 | 495 |
| **imp-az-hier-nopk** | 246,657 | 452 |
| imp-bimodal | 305,844 | 493 |
| **imp-bimodal-nopk** | 325,211 | 450 |
| **imp-direct-ack** | 188,455 | 495 |
| imp-direct-ack-nopk | 251,351 | 452 |
| imp-flat-once | 446,821 | 495 |
| imp-flat-once-nopk | 481,916 | 452 |
| imp-gossipsub | 332,171 | 495 |
| imp-gossipsub-nopk | 418,747 | 448 |
| imp-main | 711,258 | 494 |
| imp-main-nopk | 835,052 | 452 |
| **imp-owner-agg** | 404,955 | 183 |
| imp-plumtree | 322,223 | 461 |
| **imp-plumtree-nopk** | 368,380 | 314 |
| imp-scuttlebutt | 307,531 | 786 |
| imp-scuttlebutt-nopk | 437,339 | 498 |
| imp-snow | 322,683 | 494 |
| imp-snow-nopk | 410,359 | 452 |
| imp-thicket | 680,634 | 497 |
| imp-thicket-nopk | 725,871 | 454 |
| **local-only** | 0 | 9,984 |
| owner-agg | 894,713 | 189 |
| plumtree | 2,748,245 | 452 |
| scuttlebutt | 505,640 | 497 |
| snow | 2,837,763 | 452 |
| thicket | 3,262,814 | 512 |

## low-5

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 25,906,814 | 2 |
| az-hier | 557,700 | 2 |
| bimodal | 775,236 | 2 |
| direct-ack | 571,897 | 2 |
| flat-once | 5,118,254 | 2 |
| gossipsub | 1,795,054 | 2 |
| **imp-az-hier** | 486,095 | 2.33 |
| **imp-az-hier-nopk** | 556,657 | 2 |
| imp-bimodal | 657,239 | 2.33 |
| imp-bimodal-nopk | 685,375 | 2 |
| imp-direct-ack | 492,226 | 2.33 |
| imp-direct-ack-nopk | 571,787 | 2 |
| imp-flat-once | 1,247,982 | 2.33 |
| imp-flat-once-nopk | 1,314,038 | 2 |
| imp-gossipsub | 1,585,272 | 2.33 |
| imp-gossipsub-nopk | 1,795,054 | 2 |
| imp-main | 3,153,283 | 2.33 |
| imp-main-nopk | 3,440,153 | 2 |
| **imp-owner-agg** | 682,620 | 0 |
| imp-plumtree | 791,105 | 2 |
| imp-plumtree-nopk | 839,335 | 1 |
| imp-scuttlebutt | 571,538 | 4.33 |
| imp-scuttlebutt-nopk | 707,206 | 3 |
| imp-snow | 785,493 | 2.33 |
| imp-snow-nopk | 891,293 | 2 |
| imp-thicket | 1,620,658 | 2.33 |
| imp-thicket-nopk | 1,649,259 | 2 |
| **local-only** | 0 | 57 |
| owner-agg | 1,782,404 | 2 |
| plumtree | 5,040,479 | 2 |
| scuttlebutt | 831,359 | 3 |
| snow | 5,201,891 | 2 |
| thicket | 6,119,567 | 2 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 25,906,814 | 11.7 |
| az-hier | 557,700 | 11.7 |
| bimodal | 775,236 | 10 |
| direct-ack | 571,897 | 11.7 |
| flat-once | 5,118,254 | 11.7 |
| gossipsub | 1,795,054 | 8.33 |
| **imp-az-hier** | 486,095 | 13.3 |
| **imp-az-hier-nopk** | 556,657 | 11.7 |
| imp-bimodal | 657,239 | 13.3 |
| imp-bimodal-nopk | 685,375 | 10 |
| imp-direct-ack | 492,226 | 13.3 |
| imp-direct-ack-nopk | 571,787 | 11.7 |
| imp-flat-once | 1,247,982 | 13.3 |
| imp-flat-once-nopk | 1,314,038 | 11.7 |
| imp-gossipsub | 1,585,272 | 15 |
| imp-gossipsub-nopk | 1,795,054 | 8.33 |
| imp-main | 3,153,283 | 13.3 |
| imp-main-nopk | 3,440,153 | 11.7 |
| **imp-owner-agg** | 682,620 | 3.33 |
| imp-plumtree | 791,105 | 10 |
| imp-plumtree-nopk | 839,335 | 11.7 |
| imp-scuttlebutt | 571,538 | 16.7 |
| imp-scuttlebutt-nopk | 707,206 | 13.3 |
| imp-snow | 785,493 | 13.3 |
| imp-snow-nopk | 891,293 | 11.7 |
| imp-thicket | 1,620,658 | 13.3 |
| imp-thicket-nopk | 1,649,259 | 11.7 |
| **local-only** | 0 | 262 |
| owner-agg | 1,782,404 | 8.33 |
| plumtree | 5,040,479 | 11.7 |
| scuttlebutt | 831,359 | 13.3 |
| snow | 5,201,891 | 11.7 |
| thicket | 6,119,567 | 10 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 25,906,814 | 483 |
| az-hier | 557,700 | 483 |
| bimodal | 775,236 | 483 |
| direct-ack | 571,897 | 483 |
| flat-once | 5,118,254 | 483 |
| gossipsub | 1,795,054 | 483 |
| **imp-az-hier** | 486,095 | 494 |
| **imp-az-hier-nopk** | 556,657 | 483 |
| imp-bimodal | 657,239 | 494 |
| imp-bimodal-nopk | 685,375 | 482 |
| imp-direct-ack | 492,226 | 494 |
| **imp-direct-ack-nopk** | 571,787 | 483 |
| imp-flat-once | 1,247,982 | 494 |
| imp-flat-once-nopk | 1,314,038 | 483 |
| imp-gossipsub | 1,585,272 | 494 |
| imp-gossipsub-nopk | 1,795,054 | 483 |
| imp-main | 3,153,283 | 494 |
| imp-main-nopk | 3,440,153 | 483 |
| **imp-owner-agg** | 682,620 | 187 |
| imp-plumtree | 791,105 | 415 |
| imp-plumtree-nopk | 839,335 | 332 |
| imp-scuttlebutt | 571,538 | 1,243 |
| imp-scuttlebutt-nopk | 707,206 | 954 |
| imp-snow | 785,493 | 494 |
| imp-snow-nopk | 891,293 | 483 |
| imp-thicket | 1,620,658 | 498 |
| imp-thicket-nopk | 1,649,259 | 486 |
| **local-only** | 0 | 9,987 |
| owner-agg | 1,782,404 | 190 |
| plumtree | 5,040,479 | 483 |
| scuttlebutt | 831,359 | 954 |
| snow | 5,201,891 | 483 |
| thicket | 6,119,567 | 704 |

## prod-4x

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 44,054,792,183 | 461 |
| az-hier | 23,199,086,271 | 8 |
| bimodal | 15,798,770,205 | 2 |
| direct-ack | 16,569,613,017 | 2 |
| flat-once | 67,788,825,893 | 5 |
| gossipsub | 139,040,206,813 | 2 |
| **imp-az-hier** | 7,601,665,209 | 17 |
| imp-az-hier-nopk | 16,105,046,431 | 2 |
| imp-bimodal | 8,165,740,351 | 17 |
| imp-bimodal-nopk | 15,734,072,639 | 2 |
| imp-direct-ack | 7,624,181,567 | 17 |
| imp-direct-ack-nopk | 16,569,607,789 | 2 |
| imp-flat-once | 55,606,254,283 | 25 |
| imp-flat-once-nopk | 70,498,282,433 | 5 |
| imp-gossipsub | 66,544,016,294 | 17 |
| imp-gossipsub-nopk | 139,043,097,636 | 2 |
| imp-main | 68,840,308,851 | 18 |
| imp-main-nopk | 78,117,002,337 | 3 |
| **imp-owner-agg** | 9,153,391,011 | 1 |
| imp-plumtree | 12,002,658,561 | 2 |
| imp-plumtree-nopk | 11,998,664,835 | 2 |
| imp-scuttlebutt | 7,845,261,938 | 137 |
| imp-scuttlebutt-nopk | 11,267,262,093 | 16.3 |
| imp-snow | 9,158,883,335 | 17 |
| imp-snow-nopk | 24,752,419,439 | 2 |
| imp-thicket | 16,820,825,391 | 17.3 |
| imp-thicket-nopk | 20,798,156,771 | 3 |
| **local-only** | 0 | 884 |
| owner-agg | 38,307,826,827 | 8 |
| plumtree | 10,897,953,375 | 3 |
| scuttlebutt | 12,891,467,698 | 16.3 |
| snow | 25,081,643,231 | 2 |
| thicket | 31,877,184,185 | 3 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 44,054,792,183 | 513 |
| az-hier | 23,199,086,271 | 10.7 |
| bimodal | 15,798,770,205 | 3.67 |
| direct-ack | 16,569,613,017 | 2.33 |
| flat-once | 67,788,825,893 | 7.67 |
| gossipsub | 139,040,206,813 | 3 |
| **imp-az-hier** | 7,601,665,209 | 24 |
| imp-az-hier-nopk | 16,105,046,431 | 2.33 |
| imp-bimodal | 8,165,740,351 | 24 |
| imp-bimodal-nopk | 15,734,072,639 | 2.33 |
| imp-direct-ack | 7,624,181,567 | 24 |
| imp-direct-ack-nopk | 16,569,607,789 | 2.33 |
| imp-flat-once | 55,606,254,283 | 35 |
| imp-flat-once-nopk | 70,498,282,433 | 6.67 |
| imp-gossipsub | 66,544,016,294 | 24 |
| imp-gossipsub-nopk | 139,043,097,636 | 2.67 |
| imp-main | 68,840,308,851 | 24.3 |
| imp-main-nopk | 78,117,002,337 | 3.67 |
| **imp-owner-agg** | 9,153,391,011 | 1 |
| imp-plumtree | 12,002,658,561 | 3.33 |
| imp-plumtree-nopk | 11,998,664,835 | 2.67 |
| imp-scuttlebutt | 7,845,261,938 | 152 |
| imp-scuttlebutt-nopk | 11,267,262,093 | 21.7 |
| imp-snow | 9,158,883,335 | 24 |
| imp-snow-nopk | 24,752,419,439 | 2.33 |
| imp-thicket | 16,820,825,391 | 24 |
| imp-thicket-nopk | 20,798,156,771 | 4 |
| **local-only** | 0 | 827 |
| owner-agg | 38,307,826,827 | 14 |
| plumtree | 10,897,953,375 | 4 |
| scuttlebutt | 12,891,467,698 | 21.7 |
| snow | 25,081,643,231 | 2.67 |
| thicket | 31,877,184,185 | 4 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 44,054,792,183 | 9,975 |
| az-hier | 23,199,086,271 | 9,859 |
| bimodal | 15,798,770,205 | 143 |
| direct-ack | 16,569,613,017 | 144 |
| flat-once | 67,788,825,893 | 9,599 |
| gossipsub | 139,040,206,813 | 144 |
| **imp-az-hier** | 7,601,665,209 | 494 |
| imp-az-hier-nopk | 16,105,046,431 | 144 |
| imp-bimodal | 8,165,740,351 | 494 |
| imp-bimodal-nopk | 15,734,072,639 | 144 |
| **imp-direct-ack** | 7,624,181,567 | 494 |
| imp-direct-ack-nopk | 16,569,607,789 | 144 |
| imp-flat-once | 55,606,254,283 | 1,152 |
| imp-flat-once-nopk | 70,498,282,433 | 952 |
| imp-gossipsub | 66,544,016,294 | 496 |
| imp-gossipsub-nopk | 139,043,097,636 | 144 |
| imp-main | 68,840,308,851 | 542 |
| imp-main-nopk | 78,117,002,337 | 3,209 |
| **imp-owner-agg** | 9,153,391,011 | 185 |
| imp-plumtree | 12,002,658,561 | 45.5 |
| **imp-plumtree-nopk** | 11,998,664,835 | 43.7 |
| imp-scuttlebutt | 7,845,261,938 | 3,254 |
| imp-scuttlebutt-nopk | 11,267,262,093 | 553 |
| imp-snow | 9,158,883,335 | 497 |
| imp-snow-nopk | 24,752,419,439 | 146 |
| imp-thicket | 16,820,825,391 | 512 |
| imp-thicket-nopk | 20,798,156,771 | 162 |
| **local-only** | 0 | 9,990 |
| owner-agg | 38,307,826,827 | 186 |
| **plumtree** | 10,897,953,375 | 169 |
| scuttlebutt | 12,891,467,698 | 551 |
| snow | 25,081,643,231 | 146 |
| thicket | 31,877,184,185 | 248 |

## prod-peak

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 12,954,553,129 | 415 |
| az-hier | 2,603,038,129 | 3.67 |
| bimodal | 2,764,606,516 | 3.67 |
| direct-ack | 2,700,257,635 | 3.67 |
| flat-once | 10,938,776,429 | 7.67 |
| gossipsub | 27,212,451,123 | 3.67 |
| **imp-az-hier** | 1,519,913,554 | 15 |
| imp-az-hier-nopk | 2,597,576,533 | 3.67 |
| **imp-bimodal** | 1,659,405,584 | 14.3 |
| imp-bimodal-nopk | 2,592,506,339 | 3.67 |
| **imp-direct-ack** | 1,532,907,237 | 14.7 |
| imp-direct-ack-nopk | 2,700,257,288 | 3.67 |
| imp-flat-once | 10,343,482,157 | 28.7 |
| imp-flat-once-nopk | 11,140,923,160 | 7.33 |
| imp-gossipsub | 13,434,084,476 | 14.7 |
| imp-gossipsub-nopk | 27,376,947,405 | 3.67 |
| imp-main | 11,241,690,251 | 15.7 |
| imp-main-nopk | 12,482,058,109 | 4.33 |
| **imp-owner-agg** | 2,023,410,341 | 1 |
| imp-plumtree | 2,723,967,168 | 2 |
| imp-plumtree-nopk | 2,718,357,514 | 1 |
| **imp-scuttlebutt** | 1,330,905,772 | 125 |
| imp-scuttlebutt-nopk | 1,804,200,789 | 29 |
| imp-snow | 2,097,332,452 | 14.7 |
| imp-snow-nopk | 4,023,715,070 | 3.67 |
| imp-thicket | 3,587,215,995 | 15.7 |
| imp-thicket-nopk | 4,098,677,997 | 4 |
| **local-only** | 0 | 894 |
| owner-agg | 6,973,531,861 | 7.67 |
| plumtree | 2,471,249,376 | 4 |
| scuttlebutt | 2,093,348,651 | 28.7 |
| snow | 4,282,873,946 | 3.67 |
| thicket | 5,732,398,135 | 4 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 12,954,553,129 | 452 |
| az-hier | 2,603,038,129 | 5.33 |
| bimodal | 2,764,606,516 | 5.33 |
| direct-ack | 2,700,257,635 | 5.33 |
| flat-once | 10,938,776,429 | 10 |
| gossipsub | 27,212,451,123 | 5.33 |
| **imp-az-hier** | 1,519,913,554 | 24.3 |
| imp-az-hier-nopk | 2,597,576,533 | 5.33 |
| imp-bimodal | 1,659,405,584 | 24.3 |
| imp-bimodal-nopk | 2,592,506,339 | 5.33 |
| imp-direct-ack | 1,532,907,237 | 24.3 |
| imp-direct-ack-nopk | 2,700,257,288 | 5.33 |
| imp-flat-once | 10,343,482,157 | 42.3 |
| imp-flat-once-nopk | 11,140,923,160 | 10 |
| imp-gossipsub | 13,434,084,476 | 24.3 |
| imp-gossipsub-nopk | 27,376,947,405 | 5.33 |
| imp-main | 11,241,690,251 | 26.3 |
| imp-main-nopk | 12,482,058,109 | 6 |
| **imp-owner-agg** | 2,023,410,341 | 1.33 |
| imp-plumtree | 2,723,967,168 | 2 |
| imp-plumtree-nopk | 2,718,357,514 | 2.33 |
| **imp-scuttlebutt** | 1,330,905,772 | 150 |
| imp-scuttlebutt-nopk | 1,804,200,789 | 43.7 |
| imp-snow | 2,097,332,452 | 24.3 |
| imp-snow-nopk | 4,023,715,070 | 5.33 |
| imp-thicket | 3,587,215,995 | 26 |
| imp-thicket-nopk | 4,098,677,997 | 5.67 |
| **local-only** | 0 | 844 |
| owner-agg | 6,973,531,861 | 13 |
| plumtree | 2,471,249,376 | 5.67 |
| scuttlebutt | 2,093,348,651 | 43.7 |
| snow | 4,282,873,946 | 4.67 |
| thicket | 5,732,398,135 | 5 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 12,954,553,129 | 9,680 |
| az-hier | 2,603,038,129 | 301 |
| bimodal | 2,764,606,516 | 300 |
| direct-ack | 2,700,257,635 | 300 |
| flat-once | 10,938,776,429 | 9,204 |
| gossipsub | 27,212,451,123 | 301 |
| **imp-az-hier** | 1,519,913,554 | 495 |
| imp-az-hier-nopk | 2,597,576,533 | 301 |
| **imp-bimodal** | 1,659,405,584 | 494 |
| imp-bimodal-nopk | 2,592,506,339 | 300 |
| **imp-direct-ack** | 1,532,907,237 | 494 |
| imp-direct-ack-nopk | 2,700,257,288 | 300 |
| imp-flat-once | 10,343,482,157 | 1,076 |
| imp-flat-once-nopk | 11,140,923,160 | 839 |
| imp-gossipsub | 13,434,084,476 | 495 |
| imp-gossipsub-nopk | 27,376,947,405 | 301 |
| imp-main | 11,241,690,251 | 523 |
| imp-main-nopk | 12,482,058,109 | 370 |
| **imp-owner-agg** | 2,023,410,341 | 184 |
| **imp-plumtree** | 2,723,967,168 | 36.2 |
| **imp-plumtree-nopk** | 2,718,357,514 | 36.8 |
| **imp-scuttlebutt** | 1,330,905,772 | 2,972 |
| imp-scuttlebutt-nopk | 1,804,200,789 | 1,037 |
| imp-snow | 2,097,332,452 | 497 |
| imp-snow-nopk | 4,023,715,070 | 302 |
| imp-thicket | 3,587,215,995 | 513 |
| imp-thicket-nopk | 4,098,677,997 | 318 |
| **local-only** | 0 | 9,990 |
| owner-agg | 6,973,531,861 | 186 |
| plumtree | 2,471,249,376 | 323 |
| scuttlebutt | 2,093,348,651 | 1,038 |
| snow | 4,282,873,946 | 304 |
| thicket | 5,732,398,135 | 351 |

## prod-typical

Seeds 1, 2, 3.

| Solution | bytes_total | stale_limited_p99 |
|---|---|---|
| main | 5,009,330,753 | 177 |
| az-hier | 423,282,527 | 4 |
| bimodal | 468,757,595 | 4 |
| direct-ack | 440,277,022 | 4 |
| flat-once | 1,622,725,223 | 8 |
| gossipsub | 3,243,243,627 | 4 |
| **imp-az-hier** | 262,560,749 | 13.7 |
| imp-az-hier-nopk | 422,791,335 | 4 |
| imp-bimodal | 288,623,232 | 13.7 |
| imp-bimodal-nopk | 425,635,939 | 4 |
| imp-direct-ack | 264,278,748 | 13.7 |
| imp-direct-ack-nopk | 440,275,595 | 4 |
| imp-flat-once | 1,478,573,107 | 25 |
| imp-flat-once-nopk | 1,578,542,351 | 8.33 |
| imp-gossipsub | 1,888,330,799 | 13.7 |
| imp-gossipsub-nopk | 3,233,823,668 | 4 |
| imp-main | 1,773,288,701 | 14 |
| imp-main-nopk | 1,932,413,061 | 4.33 |
| **imp-owner-agg** | 352,280,440 | 1 |
| imp-plumtree | 523,412,742 | 2 |
| imp-plumtree-nopk | 524,251,195 | 1 |
| **imp-scuttlebutt** | 221,727,951 | 84.7 |
| imp-scuttlebutt-nopk | 296,896,720 | 26.3 |
| imp-snow | 371,173,480 | 13.7 |
| imp-snow-nopk | 655,754,553 | 4 |
| imp-thicket | 668,377,097 | 13.7 |
| imp-thicket-nopk | 772,694,923 | 4.33 |
| **local-only** | 0 | 892 |
| owner-agg | 1,291,009,110 | 7.67 |
| plumtree | 536,159,197 | 4.33 |
| scuttlebutt | 345,581,845 | 25.7 |
| snow | 788,757,421 | 4.33 |
| thicket | 1,114,007,492 | 4.33 |

Seeds 1, 2, 3.

| Solution | bytes_total | over_max_pct |
|---|---|---|
| main | 5,009,330,753 | 209 |
| az-hier | 423,282,527 | 6.33 |
| bimodal | 468,757,595 | 6.33 |
| direct-ack | 440,277,022 | 6.33 |
| flat-once | 1,622,725,223 | 13 |
| gossipsub | 3,243,243,627 | 6.33 |
| **imp-az-hier** | 262,560,749 | 19 |
| imp-az-hier-nopk | 422,791,335 | 6.33 |
| imp-bimodal | 288,623,232 | 19 |
| imp-bimodal-nopk | 425,635,939 | 6.33 |
| imp-direct-ack | 264,278,748 | 19 |
| imp-direct-ack-nopk | 440,275,595 | 6.33 |
| imp-flat-once | 1,478,573,107 | 34 |
| imp-flat-once-nopk | 1,578,542,351 | 13 |
| imp-gossipsub | 1,888,330,799 | 19 |
| imp-gossipsub-nopk | 3,233,823,668 | 6.33 |
| imp-main | 1,773,288,701 | 20 |
| imp-main-nopk | 1,932,413,061 | 7.33 |
| **imp-owner-agg** | 352,280,440 | 1 |
| imp-plumtree | 523,412,742 | 2.33 |
| imp-plumtree-nopk | 524,251,195 | 3 |
| **imp-scuttlebutt** | 221,727,951 | 102 |
| imp-scuttlebutt-nopk | 296,896,720 | 36 |
| imp-snow | 371,173,480 | 19 |
| imp-snow-nopk | 655,754,553 | 6.33 |
| imp-thicket | 668,377,097 | 19 |
| imp-thicket-nopk | 772,694,923 | 6.33 |
| **local-only** | 0 | 840 |
| owner-agg | 1,291,009,110 | 10.3 |
| plumtree | 536,159,197 | 6.67 |
| scuttlebutt | 345,581,845 | 36 |
| snow | 788,757,421 | 6.33 |
| thicket | 1,114,007,492 | 7 |

Seeds 1, 2, 3.

| Solution | bytes_total | conv_p99_incl_never_ms |
|---|---|---|
| main | 5,009,330,753 | 6,239 |
| az-hier | 423,282,527 | 337 |
| bimodal | 468,757,595 | 336 |
| direct-ack | 440,277,022 | 337 |
| flat-once | 1,622,725,223 | 9,256 |
| gossipsub | 3,243,243,627 | 337 |
| **imp-az-hier** | 262,560,749 | 495 |
| imp-az-hier-nopk | 422,791,335 | 337 |
| **imp-bimodal** | 288,623,232 | 494 |
| imp-bimodal-nopk | 425,635,939 | 336 |
| **imp-direct-ack** | 264,278,748 | 494 |
| imp-direct-ack-nopk | 440,275,595 | 337 |
| imp-flat-once | 1,478,573,107 | 1,057 |
| imp-flat-once-nopk | 1,578,542,351 | 913 |
| imp-gossipsub | 1,888,330,799 | 495 |
| imp-gossipsub-nopk | 3,233,823,668 | 338 |
| imp-main | 1,773,288,701 | 518 |
| imp-main-nopk | 1,932,413,061 | 413 |
| **imp-owner-agg** | 352,280,440 | 185 |
| **imp-plumtree** | 523,412,742 | 33.7 |
| **imp-plumtree-nopk** | 524,251,195 | 30.8 |
| **imp-scuttlebutt** | 221,727,951 | 2,620 |
| imp-scuttlebutt-nopk | 296,896,720 | 1,046 |
| imp-snow | 371,173,480 | 496 |
| imp-snow-nopk | 655,754,553 | 338 |
| imp-thicket | 668,377,097 | 509 |
| imp-thicket-nopk | 772,694,923 | 349 |
| **local-only** | 0 | 9,990 |
| owner-agg | 1,291,009,110 | 186 |
| plumtree | 536,159,197 | 355 |
| scuttlebutt | 345,581,845 | 1,039 |
| snow | 788,757,421 | 338 |
| thicket | 1,114,007,492 | 417 |

