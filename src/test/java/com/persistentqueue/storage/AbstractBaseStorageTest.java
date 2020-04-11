package com.persistentqueue.storage;

public abstract class AbstractBaseStorageTest {
    protected String path = "./pbq";
    protected String name = "pbqtest";
    protected String dataFileExt = ".dat";
    protected String metaFileExt = ".meta";
    protected String indexFileExt = ".index";

    protected int initialSize = 4096;

    protected String oneKBText = "v8v6OIuhhALPKqA8W1ffq9ibnGHhJzfeG86PvtTjXFDfVYas5diIgqyTNyERCoiK3d0KVnvGjqcPthDjb9n8zkgXy9sg3ew30tcpQBRmNQ0YYjis86qVVlUco3MtKl8HMDYtEkdOhqdwzxB0j1boJH7E3xPDmm4atzLeVZaGviKeFecU8QiwDLQNZscb9wvEniPGeL8dRrd0hq39tjibBdvTnZuweAFX2jHaDWozhxPrbEEX9t9uk1n0aC1ocLz7RHASDVGswYIJm0wHeyzpOpV5k9ASGYATUkHArl2DXgLxhV3WgWES72wbqm0fSw2pN8h6rLHG18gPnQr0v7n4dx3DhzVvyZ2KvjFspoaKZhMBotpYdd0NatHTHN6oR4zAsx9u5nSFl0rIv18upGinWf0l1UyPH4pvPm1LIzqAldyaIsQ9GwWBGl0Hv4ARN0W7bQHamBgty4kfNOtBNDN9QKO9qszcylpzt67q5QG0gmpz8hZ87k6fGi3wsPGcnUDCBMp7YTXSS1ojYNg8BhS0WN1M412T5jPfb1v4HBPJwreNo7JqRGt0yNg6XsHVre5O9wsa5Fppp5d8WFqnPSERlSUShCisWbLXl3DglgvXJg2GVCZlaGNe5cRrkX2taHpNOw3lRUfLoHVZ1aABFmMaXu3vwsbsAxyvda3swFSJcFzC28LqlR4S1cMKRfHanLmgmm1bDM2cTbpeqFwfSuttIkky01F8XdGmbbel8GkbhdT1zgNY2oM6dmJ17ZsoFylfOK42qOf7E55ZCWcr1AMdceM41aGUnhshIjGwT6PTgvhM6CAn3hy7FMv4CMlomW7onwNfIxaLPFWLHfTWWdRKxv9G4bX2pMydKktvEjusCAtQNh7U1gNvtrFkDD622XSQTMp6KADRMHZzInQZIy0HkfJF2K6MloEOMOOqQ31SesYUneyjWxFd5D7intnIGnwgf5UjJb9G82gIEOAbt8AKavMy8qpgxuvXGXGjGowvIR5KFVQwva8YyJ1li2vvRzZe";

    protected String twoKBText = "TJl3VxBpfDOnocOKG429P7FFNzrvXrToBhwb1AbPcd5V2f4Lda63VEanLJhetcmxBh0fcf6p4QIUNW6qgR1JzgVNtvp6zQiKLLjHXLsMi7WUUTajC82e47yh0ph45UFFT4lqoHDDykrAiNWQpVdZnkRi65g6ld5W4uPLvUUmUenIoyDHkPoUWTLVse7HIq70JLa2ReDDiXUTToGAr3LG6O0W1JF07mPvPPn5qwATnUW54nbTtHgCQQiN4CYKVEympBAj6suvafbC6Mdhdg4SFGnOpYzsT5QcBiaSlgKtSeAgpaOhfUyjekWyQreU1oG943bUKlRdfYGHEP0znC7GGFSvwg0QbI8hU4ev3c7t6MdlitegyNANCs1S6KC7T03R6uQWMGEFYV70OwA5WMEHG881ztnWvwgq6yQKPy29as5lqaEq8iVmR7HyX69PGpNYXsvfTKkBbthUz1sH1kmEmlfoi3YejLzmOFhHj3HGHzPVX83BDr0P0UyglmaXQJp0a8dCX7LF5DlFCR5OCbxeaX1Gv8pBkRRs8VlEZ253PxRSqCpjlvnCY6OFjMR3D9B1zJ9oL4yvXRyLMG10qijXubZYhckrbKHZFSzD0B3HHvfpKUhNyHtZfA0PPeWRWzvxDNvBhrgWE12yTaM0UNSSqSI3H3PgpVFVVTyFjcG8UteLsUlTvS0vrGcBX11awU9W1epxwPwQ1DVMH99UdJ6qm9W8kPWM8K180Yax6gWs3DSOFHxqVF1PXopbBJ3uF6idUa3GETOCl5BJsgS1SNRVseA76ioH5z8oMOml3wg0zdjPrmHpVgLYy7zzVSTaSDCKByX52iLLY9Iswqx9DiOGIsDnwWLHkqja70zpeBG2iVGSw9wr336qyKhZ8oFngJIPbFbYmZGAavMHN5nTjDYFUETWOKD8nI4JjYSxxgV8WDw8sSMjId5A01xgUU2DTEbYIIXlShe6bWOhiKO4U0WOeNEBFmGSGBGWHpeHazFmsbz2wD7dJWUlWEHJgos5brYz7" +
            "9i3UiTzzULJesxxvPcLFRoB2EXzZZWaClsWyTut4m6XRfJ6VcxX7CCSWUyJv62TTTYyMItDh8xPrwghplbfBR8qt16P6tevEZQ1BH5sXhlxd7pLAfcygQTpYyijHj2mR9WyYT5p61llmnz7mVOOR8vRAxYonXgGwLqIA7xjaYnXPAZUiNcwklbqyomOSGU3zGwCJH11NGPeRyYNzvNe7tefjeyd34MTupJHfn7ym7gqYEmDdkx8N7YxYPG1ygmiJvQk0CvHSVHLgP4OQ8biVjfMI5UVOTm38pOcF0WwhCcpNuUewvcEeZM2B5bD8rsLyxJv4ZmCFhWsKh8NXbtgrOQNCHLmZqutYnaFJ6CGKzIxMbiJ0Bi2JmDKiTz9uajjBFMpfDLDQdfgEBGfUhn3EV8YbvSO5I5FgFpQJDi6izx7I3IaqBjLAM9wN6FKugG5HFwX2sCK2UjLceckyNyUE3hg7ueEZeemFLbCcfZVix8YTTF5TQvOYnBziMbxuN4s6lsM4gl8ElxsX5EQHNygBMo1rKYV4nF6BAJnA9AMyjGbpVSBoU796daVt2yoOMvDa1ARCScT1euLxqaP0bmKcCSE1fknlsOrToyGArknP04nCxM60wux2r7OdCOyn9nX8SukDxPn7Icm7nZfjtoQDQk8Cct0ZUPDT9f82WbGeniUSJE4qvrJWLB41nPMLJU08YFNspRmKGl7E9zYmCfJC9EUEPkFjb792kLJQMj0RQKAhI2Q1pbDtSC5StPbrvYJNnMg4pef7R5weXY5kKAXBIQ1Y7Xn6sPG6P4tBHJwJxc84EMUCa90gMcsOuwbVZogm660QSeQ9Ugid8dkEuTpeSQzVXNLoO8YZVCy0bLK059BaO06u7T4iGWjoUwst32gZt9A3rhxJN4RzUAWoqndJsCrgyKvpgLksZjIVKwr2oDcZtmJotcI7PInKTdPQAevzPumpnzW3otKX9sKKqNF6Nn8F0lXnP0eIYQYZYFsnRKSKE5qhl3HBEEWREiwQtc9";

    protected String threeKBText = "CVljqH3Q0wIH21R17AeBqdmgxZyxI1CdbBODb4yCOWNflqZGgdVnKf2tFCSP4NkAmOhDWmfJZrNrcx45qsrtsABksabGIZjpM9RmCvsB9rkbpEXRVZFZ11rbN4xXetrWlTaM3mxO6jjjkrKO5XvHWPIJbWEMbWwjFYKACtkjoXyHM1fPE7kM4q0JYl43wUsbQUSnSbrRA5WoS7plhLowOgouUcg5Y4eQuI687oopaaGzXr4OcwK6uWNuWLq4N37iC8hIA2nAH1WagrxhCarFQifMjH79UE7MVggew7tALDJ9CFvleAlpgvyAygbXvpYTWiNlEUjAZH2hmk5pAPoNlPL8arVssW2cmOmYmW2oLIFWQGpt6ARgcGy3rjVbI4Tw1ar9cMiDvf2UCSbtiJr4BCZXfrfTI7VojvxKSTeK0hajRmx35JsQgkUsPgW2DUWkzopOug9qgfmAc95eylrrS61U2brqP3OPGbHYvrJtPoKuEc4KM4BDjCuZi5AZi53DauTgevMjz64Z9Ox1rjK6MjLPumRcY4SxCHubHDjKuNM5NEBSA8iQi47JQOTHOAAxPmy2bxk9REvZcLgLS4TtN8eHZdPXyTig9IDoxDGKGg2zd6ebIIGoQLHE66aGBgCwZO3ZjwxQU4mK2J1pYyNzGaTpD81CdeWn8nVl0xZ6ID8RcmI5Jx1aA7lr0ygBfa993Ykg7i56oVsLX0DQ1ULn53INTGKKV6FnchanNDvTA5GyIZve22iL6faPhlxV0yxVdxbcEf2NOxVFVi1ICwGwuFJCNvmlSxybkq8BYeVxeaju3cM3AejgojlgFCGsPEYzI1NgLIpbEPYXuJlmxw5WoIRF31RorReR9qlYBfPbSV2U0pTUIBAZ9Dp8Zh6ZKDtdHSCuYi1VV9Ygsq3zV9xG6qZtBixozuhvgcSqQu68OnH0iy8HtHmqFV2aKuCMDHvCFcKZqKjDfFrRvVS2pjK90evXhI6p89MHaf440oUPeYLorBXiPWG1t7euLTW6RbMGI" +
            "2ljpC5I7Dc2ACaKQXFu4xEBRj4ViBkCcFRAwHEyrfzg0OY3bJQeIE78xhuqwfoIgf3CBmAozYAuteIcqQYtIJjqLpQjzItHKqMxdUIRe1KH7b5XjQ9PfHLsrlx3GcR5IbJbWS5PUBnU5HR8H9XqNGlug4oNJUAX4bBz92uyB6j4qcRJHTv8NQBJakDCMTTYVKCTghssCF22W1302TeU25HWBcwSk0m3unnTJYrxBTq903Y5hu4Tu3efaBdaqR0mBxkwL4iDmtAgINiAO98rF4YHGeXdA5MBsdnsK5zJgYlUpOYZ3pkHonq6SO7gCWQH2lP7dMMXwB2reITbUC3c9DstmjJpYk6jiMguKClmJeFfWmxbGNjNmDWc6wgpjiAseMOHh1ghYnUMIguBMaJUubRp1geQef5JH49gzlGz5SnUEx2UvksLxKT0glhlsr50QV3J5Yi9Uc5SCTIKoW8LzEtqyrmMXXrqLYkOxBlUsuojWms4jKm4XXgF8OL1Nvgv8KHK4wpHGYxui1Cl7XVav1OWaso74X7xRKDo4X3GUBV2UgIoQiZldYKijWNN2DwoDXkS3u1ChLGwLomeeOfQ3dOcPEfZweRyQjTs4nXB3296C7PS9m5mNEBcfOqnDzGBFoYcFNGijoAinMjlQUEK7lpmNhBIAUwd7idMeCUxGkICvEGTlNk9JORRMxyCc54TlwyYgjH7ivPtaY0iAoIKExVhT4oB3m1db8sHJe6co4ej8HKOOUynUITs0Z4VONmmLyni8cJ27OK3Jbrw1TVQyuwAHAhRMlGQd7sI5MZwXKNnMUPAZEvrn0gu6tDXmPY60x3xUnhhsZCb6PVB1BWvGOlSu5LJI0aQspTfV8OZvAQYV2UA0mmpOlG3PXiFXwy7N5Ey05fFhSRlkRFy3ppCRx63yqLh7PrwZBdtlAGBxczXhPCg1kWNBrZsEekqcj7nuspRuwPkgz5akkWwCCDP81KNbu2oc8pFu33Zrz5y8Sbvn5LysMsmGVGTeXbhXDx9Io" +
            "dlQKFHm1lBtCCqTHSYrtdbBNVzVxj5Vbewo1CDefdr9fhY7Q3nWrZaClf1ulvF34kcUDgJ05f7Ih4CMfEifhVBbWXJ10Tv81qYuklsaYAwumdLAjJBgWP2HYVBNl1fBV0INYm7UsR6i1TDKzaFogVPZ0Hm29JPE3jZBDTkkKfQcDJOkolU3fmyDXp9c9t6PO2flT428KBnIqEor1wfQlxfwd6coDf1OlKR049OaMrLwOSsn6ECkoajmcSpbgd5LPF9juZQHS1WkfP6Kn0Y4occcVCENzTTYnnb0AQ7BKrNJNZVoR6F5VgsmyX76k8c7pqiT8dMN4sHb87q81yiRgys1AzEI5LqbE1jGCx3ByDo7rfrJOIfHlzxObfpOYcalXUONlLx1jgvIe33Uo1oPilKT7WPrS1BqVzi6Jn2Ls8CRpjpDrJXbOrNPJvh7fy8IDfz6ckTkRyZgIm4RP54t8w3JENxd3rhG6qfM0W8LaGKSkkwRTNwKGCwLymM2qVq30L85lhrwzph68vZCZWKYkFyO82vBWZBcl0LxHFkRiCgkdIFgJ2FmiReWrut3BLuPrI0r9LdzJFYKSD19co50qnEKSsn6eMeW8Bvbrmy0Gtih3AZ6Y177d3w3mxKqVglzdYlvBX1VAE9AxXlGpr3NChFgaVT9EHZw0llu2hYYnwBnSnPq8wo4fK0Ibfu3y2DjfkaeX3GA5xVbMmU9HqzU9kmEZamgIqL7flePuRohS8nMtmu9PF2PdDgkdYBZ6y1guWSMRpwfbtcpe0JpGN7ShCAHBi5nggjmiUuUFgsKtYzvjuCwySQutk0ZncKyYrck5QbCIC5qYOLiC97IeEmtNTABR4l1Bg9Zq9OSxq341WhR9gvnMgCylnvResTuO56FeGavWcgjVPV9E6MAsPEUOr0NUfsqtzBqLkTECaxYrGNaWtE2mbfjFYATpT2AJrsCp2E2ofIDR8ulFqkSZL8lNkbeuYpTUHU5Q2PZ9ebllFyKRmMTIuqQW6zWv8MFv5o";

    protected String fourKBText = "VoYkWgg0GvkBlCS68aNSRHxlPNKHvAeEfnYRb9aAhhvtYqMDQRUwAy7Qx6gMhIwXkGGacd9bMW1zj2XWa5Q84TVRJ4tpg4HH3TZ3M4PBKcBbBMjIXjQTNUCqPpTcKtZe5LFSY2WH17ixJ4dDNKfp96kEBvorNvyn0FVvtJ9AvPQKMSmFCGs845vrcs7Kf7smzXqtN8C2qt7dvoyXBJ91meNfU4ppOYQxbJ8bhmjobRcvNqwmZRQPf0aW5qq8OCj56zDuoAP05puZWSGXE5OR7nOlTe5GPAG26bBQpFALSuYtl26NXG5bHGSBIyEuiUWqa7Admv3lzGJKpwuVxNSPANhK8A9j0keO5exY9pW8fMXlruvXSv1kfReytn0iXBbtmrSNdbxZAZ4OTDWPhW8KDdkgAlZQkOyZYlm8DRe9f98OkrMVwKGl27Zsgskv2iZmECEkRMlBPjhGCi4A6CGEUg1IT8DA5qFQZkYVJ5uVRWq2QPK1Ae7OCf9fOZkBm1qmvZOWcwYTdqiTAGIksJZQACa2p09uRqsHcsx6ndjBbKbGI69qLN4eOYXEHKFqdra2Gi5eNJhZJLba3ViFJFCNli37lqIr4M56uuVXww35IFerVyfFVEwCyNLGf639V1h8uIUgbjfaehyuk4jgShc7ay0ETkhTQXseynNl1Kqa4jTbxzPKO7f9weTA116cqwNQ6gG4ZXIoKHBLSaj2Pebd8Aa9Qkwg12WZJWxUVpdc3ltn4MNn2gfzWMdpAL8GSw9ufmVKCsotwaKJPR2S9C31Zl4gIjUnPrlvTFHLd8RIc7673QipTHqbGdWWwFZePjEuYRuKXy0v3hF0kozjVw2clCmFQlVwzpgtmn7XBDcitOM77VTzijxBICZxaqhrMkmJI5ILT4Ly6GfrGI1WqJmNKl3OyVb0VsMa1MTUX3r52XF4PlPJjVOziQcjU6Wy93MMb0RSTSDdp8BzdzSqrodZ8HTmT1CtdsJBgYJFL3avqGWuwY2sEI37X9C7wJJgIsdDSEZ" +
            "ZHtMhOP5svnr0ywxLHoG8IV7uzDriLTj46jtMapNavyoijB3nkVtHQHhgCxgB3O82nHkCx65stjsCwl2N2QjDJoFwvMlT6uDjehDQBMJS9wDAidxGbLOq9ajOdRPsQEGqaRdcLAyRrMcM95cCGXDSy5JXOMdyizoy4I9iPEd8Yfl3jbVwd3zjYBhqyH3Ks9NZadli86IdjW3BVW2wVziN63f1zCNAC1EtBbIOv4LHoV2o6EGHbCoTH8TaWbWARkfDspToLRFX63fMiu2KQTXudHbHr2TeCGRbuAG7nXOWS6RYyA2SQ6TBZ5sUVxjIslX09x4PtqIvqzKOGvgaHKaOFZpdTbhIgJwVH64nmAZTQheL7I3hjgVpRCnZBkic31k2HLlrCRTz5tRaWvcYGdYkHR8MFHT1acxhgmMQDChAAVAYGDdMSHCNrh70tc3ujsODrP09Z0OwboXyF2BBP4QyfQAEjGu9GUimqiJyUJAtvTP6fBeyJNzC1YdrLlxyMTyHud2PNTEB88MpfyTmH0CTpFypip2yrf0IMe2TRjUCLwa3z95W9el4q0Ow3se0lZKBYNmbuijUTqCzsNQHD7OTtMeKlw5DVYrTHcIZIcG5hZjgStEOXxykzLtfDUv03kos99YOFe9xWuB3kPvW5OWnsrB176D6NJhSWocRbqRMwO2NPj9po7TCha4h2S1aNLlNXRNA0EGWJCm91nrthHae4jRgBjdQpZMH6Kt6b0JAOzZYWkXYsCMmS0L33TVTPnJ0mZHUPRIf8WpgciHyaC8gHYymYwyOOoaBny411iUsTAYq2tTPX69Jp5BnmFmTJ4dNDpsMnQe4kEXYvBs0JwQUAfZchBdF80sh8bk0LIimOGUmoAqbu1vn4w7IsIHYUgD9242fL3CYSuPdSx4Vz1rU281Fc1X2Dpz8uji3P2MbAr4QU4B1o3tQw19jI3JY7HHtGA8wC1v8M4v9y9lfbjZ4ht1IAYJ6iQwyvOyQS4KObS4T08zMJQMz4OcC0Kw8h5HF" +
            "vyf1OJeqG9J3PA2kbN6NcDkzhuBwdmxNijJCf4rMXOMwHjGOu65fm8fU1Sb3LLwWG2QZ2UgiugJ3E9ClLb53U8oaOykc8qTCZl1yPlTxzqkGM0Hx2B5sm1p5gtXLxYfaB38KOuctqLV2ktOPv6k6mJk3lg3XQEizY0VWySvbi5nSVWhTWv9b5WmWRdEYn3JenIOMx2q4AIf7vyWjLVKrb72dO0XFNfcugz9nX73iIXJrKU7sIo5NCxmgGkLqcVoa2LcKOHae6ioJCAsNKBGOkXNpylBE9XyTWJYBdaEjIYbjHVzrS7ppafd5Xx2EHqMKK8wI0p6sD5gbDbwqubhc62s1V7cEmlh0T4JG1jCj2Zqtb8APGGjBUj63RvL4CgKKFLkMu4PK13u4vnMMi0Mf8J1k9pX2F0qHcwXygHkNqUBy47sbuwmzbfFAZRvs6VVs3CL4JI69PEc6s17OJkux24ZvIn5OtOPj0B1dLttGeBniCHS3m67e1f3ssDUoXniiItkoYGBVJ1XkH7OHe2t7tdxzMB34EGSWscPRxeqb2Xq3vUrrMrXufp3kdwRec1QOD395LjkmK1gMY9C1D1QpY41xG2hX1YJECfiQvT7vgynUzAoJoeCnx8rraGWNdlJQT8LuBdGw7hya6c0WmY2UOTmRjMnxT1F1rY6MZUzW1JB9aLY1jY5aNblZIp2Fj77Ycxn1phLP5AWiYPOIViBtrgBQzY1eNRc4KZxlubBzeu9wWFMgAICIoKEAnG3DZNfxlK6g52WizmZ962EtFvwnn4al295cbQtelLNX4nwAd9tt90GhqHy0yG2GR6EIktFaB98lgKCPgj5LEejhg4um0mfT8GtRHG2s6oJ6Yaz4WubNqOd8DfF1kiPDNhvIYsJuawRlwByakV7jAffQBsyYgerEdlfXtqQDIRrOiVzD26C9jKVY0LBHbaqxRArcnX4ioYHqV5HKAhm4kBXJxcljVpZYVtcvtT7BYpjR3hdTFVfyNWFsiUyX2SkoYIpkErLEl" +
            "496s31bLNqyjBQm2RnTP81zjLgZmXtmcASV9KOuIZXZgEdCBhpoAvOt72bCk7O6MReVR6IZ1dw9QQbtUXrKF2UpEaTlgamghI32rvnr4TfzXSgmoUsotJZqjh20jGjXNukPKBpvzCtjTkb93y7EXtdlPOuOlTVY42NgN3TpootqDhg7PWUtClvYPNTWRlF4ij7y9EQY4WucP03WYTq1jKNuTrsqDcnnWYKsbNg9YvkFLZV6sYXIc2K29euIZsbBPoB0uZ9SNSd9AGMHwC9WPJ7IOLRFSD8YvA8AWFS7HxcI7Nx8vszVmfw5IGYvE1esBFhlMlSfXYLJNbQPFpHTS8TXB6cpBF5wmLTrEzi9WfRjtClBIC7u0yKAOPfx8MhmoG9CGrm0Tq8wsPzoJhMauIggaN6Pw9e98DbhCdsrDxkrENgolMAZeNjzKyMGdaSjTxTc2bHzm3RkzbKS9rCgTFYlquuovsGJii1XTeiJXBjLtiOWHYsma09iobS6FBKvFOtXqtZiFfDdQ7AaW8XdAR4L2EJJGdm6QTYyyGMsy3qRK4Tq6ov94Mqhc5sebbJf5yEguE29jZCqpJTWZJPyESzhfaVlphJJMePpVFGJFxsOdGubbyrMZZtxj8gpZxr5nY9eZtHfGMmeGYzewXxU77wqtYdb0lyAssrnNAAYxLEG90GQeBSC72C2MFiIN0z3qmbeyUHqiHTV3LjZqaS35Z8s4iPXvsVJereejWIQV6n29Sq1JSHmD0AWHVpIIVOrI9eKpBH1YaWC1apFxdf3U6mlVBJ9BgKLTBZU80WgMHseWBV27snbeDwmq91shKVb4ceyAkmiI4qWAj0ywIhYy6HsamGSIK59iYO0QQM4PXPtAyeNKOKA5ksXQO12p1wrRYLChEJvfSYfBZWwW4xRGQVtNw1TzhYAChSUmMpSLDHjNdcmxPCbQR4GvOvKPQxty7h7aMHnwviKiJIa68u7sUi6weJSrm72bXW4y9ibumQES5FXZ1LYFXi4iacu";

    protected String fiveKBText = "AJRgelTDZpFpIH4t7Drg5VYUiXc9zGdExTVgXXtcsIt7BBSFpvtx94wuSlLSFdjWSr5hf7xixYykHarcojbuMGmXjCZpMXb265eQ2DMIEjs5anUmNgqIkeoivJHwTkK7sYwjUKukiZW8qeYS07GqH9QHYTVulXhBee70OgZ8MUVbQFCFyzW667gEkOJOGyDoRVhIbAwJnNHTSW35eNXn9T4gDkZP2ZWlOMyxdMMpnYZuGYKy0NRpXTVi69JOnKRUoVK7cxUQT7enojfNjrnQYfsCbrGk2Z4eIEygNf6nNrvty7uNsUvjHo6KM7awwNiEnaRHV04SIQhDKoFHQ27umztwkNaDKyEh5QuIPHgEwTYEYko1XH6qcaV5JXdwfBf7KdaTt5WDRNq0LGnb66qbRKgV7meUK2o8E9nXfAhJVkBckPOVjynZRZ4aswJJyshkqz0HzA6bUceDPcEpu1FKZXUnMRo3XBmo5Qf2fYvzFEmn1RdBFtYHEpItWGeK9h59HpL35CCQSfWBY3iuHldX0a7Ep7vj4FJr9U3HHAGkGq5j2rXotXda6QDB2uDubFzffKMWEGL3VUfA9oCVIucnGzFA4sCtRF7toqgnjvy5pFpU1PhDVbUosQuL9Gq0YLf0JSwagBjXqI31pp9Xl5Ayz71LcnxNPhRKiI4q8rSIeZASr0HfPnjJjcEtVRncp5BB8wMkjW3yKiMwBaPS0vijqs0WiKW8CDqFnl1OSWrvTu46SIqrWEziqjeOSSR0EuHdIG8euA7pAJzRPMbKkvWRIkDQgqztpzd5B5ECPl6eQgCk4pObbInZzhvW3T8AYNSM3BatavJY6vULD1P30Vh5t0h2fZ7cV4be3ss1IkZEPAbhQPHlNSjQ4XG2dAO7Itkt8XTGCO6Od2UvsAZvTsTt8XkHloQLdTVq5kpVLS02UVjG9KAvMzeVXQGT8K5JRX18dSN8qbDzaHY7oF1iJ0u8crn1JWBhPfJwYIJmeW6ddyCeoCFoliwMtEDK8KxIqpEKWFGlq" +
            "3M6MKm1PY0YyfRVbVhdNVi2YQb5KOMiz0XAByFvacDgcpB1qI2hpGUWOuylsG15eYScDVthnc00oGGF3FpCk5TLPYVTSIaB2FDhXZ9wfOKSymFHqaDFHN3uS2Q9UJxwSv0yrfUfV5aUB6InU7Hiel25kZMA74TMWGh1NU3Cu8SWomnbXG6wCu6mlNcDFwQwmmx6g1onb4cikzGzzK3uH1HVqwyobb6H8pvyYl0COfTQNW8Bd2QJc9YG8bDjEQ2MTgIP1wiTEUobrWrxs8lJQjmexV0zau4LIE5VUbouzkV5c2vmtRaUmgClLaxcAWGViEDK2Yz5tYoVezkt5PfeKIJgY1VrCXoUB77zWtTallGI4c1pISLJnOXorqf6sIhblA6i6rojQhWhi4sCwr0J3A0ABt3ZO8qd51QgihX06OOvOe9Z1BThCzSTGlOTEmvanCmyM7W5T583VOleKhCqrmZI426ZiOHhcfiOo5i2CHHXXMf1pyrfVtgvzzrHeKvKQQo6uD1aLAm148fOGXJtLacpbicgBH1EDz3oYSWNbo52DRDXqz5Hwguu3sEdQM8jcxXPFM1UQD1hDCH6P780DV9mGafStU4OpKRqKjNm3GY87GT5tgxK6OBzvwEYvimtL0OS572fg59sylPPBIrPKWcDtexBw2mkHe8U11TcWep29ZY38d4CwAvPOAe6o7HBsOR1y9pwcHrOFMdjaqhbjIgcSyonSa9QYT6Z2jkYF4Yv5Yo6HkPhvRnz7j4Sk2h6cjCbTpSdCuLaFPPPqVZzU2AbnED2vJegHjw31TFer2QDXOLGryl3ya5D6eulnJVYhZrWOIinlKS9QvycsbGwyjRI2aaxsqdqHaSXlNu9IHNSFZpscaTXyMGhficPs8UPKvjbbNGBsrVXRwcAcLaduQR7UopGCtX4T4boOH1J5juHhXetg60fuU6ltGnsrIa7NugORFW85HYiGnWnYJmNVkN1ytrUjPB1qhBmK1Zjiy02KRGAfbKh2zQRNo66KBOq0D1" +
            "v3f19h8vEE1ajfeMBlHMcILIr4drhaWXeP15WcORFgFCGk9GToNp2dsTtf4fcQnrI70P5nuRRtt5EklOyyxsalsquknftgsCji5fse5FZnviRIZ6wOv8ZKSOQdqqxwAaT2Ibt7WYwB9IdeLYp78PtRpT6mdfs2ScoqVN78saadf2trhgjaNFz2GIPGzeE6c8Mb7p3hgG1DU2mZHuvDw4sf6YXZDx83r4JqE5pzLDhOfskSsLYZ7RbuAFHdQJT1O11aMqtjbl1lLUwFCUxtlpM5Lr2Fu0tBSbII5Sp3nD6GJmQLUUsSE1OMSv55vqGYmp0oPR5Zj7oY3PwedVLSfcucPCb1i4nO5ojIZ3N8ZPlCseuhQtFNPdWUY7hLd9hekOvDhyswjlBwfKyK6H7JR707EbNC4XktcGq5zoAL7sJebQyHurEB7ybLLqw2XFRwsgaGvxzU7McOIHJD9WMvvcRKfBqE0xDmOWK3ioVnMSvSgaawifyGvPMDISY8kbt0BRsX6wfTDYasArXFAVfKtiLfeSm0KXH6ofQfTGtbl1WEXuqqg5vqwnVDzAEqS3xct9nb4N92XdSvVAqKnoAgMiVpJCZCK2s5bXy9PfNX5GrgwgbKybdIa4TstkVFRlMnic7PQMWBb2w1zBrFMfBnFjUxGYvyfiYUyneRjaGhU0XefsDFX9gp5zNeNT0ACEfZzoPycBqFHbDVuFYoaMkOeAu3Xkj4CTrJx8gRf7o72TBoq8WvJNAvINoyi6zLS4Pb6cQXa7NNIVbtdxKtOyCcBONZZscUlfmaGFsICpC7f1Qr0ly7qKBdnlemmIwWaQqQblYg4bQqbnQDx3nvk49d940vK7YSOWTxdFvELHyjP2L5BQuM6aRkTGe0m2RpS3DisdKX43McrtyLff6egTLYMZ2dA4QBFYnB1QwMEzeGH8gBP56R8slMdJcssZXLCk6BcgCBOKlgn6eX6aWdQKWEHvcwwTjk113x9n45Yz9LcowYBFUHmXKqkU5FhKeK33Ol8VDR" +
            "q9Z2p3VIFzGc1M1IGSZ0IhoKuZveePyNPjvpBf7hzl3tOJ6Bp6sGNQcN4wyzyfqY0RAEk8JAZ5jKmh2CLgEFwrBHjlnPsZKZmBHZuelXpPe955yPhFW700vHhcIfBkhceHHKKMq3ZRRUHzAB42U15docS0nd9qIKxWxmTrzkB20lkMGPZhk8nhZEnOQzj8WBrKc1UTqQK2vffeWx0eBXfLtxDrwcH4kluZCuTGTIqzJ8kBT65JBV5TEZY87bVJyvUSYYgFm16ZMg00AfI7PCRHsTFTws7KdIQfPIZyv4qzhYcn50lr4YjYyapRowToz3DrMJcujiiyvjbdn2y3iNPIkzCYK2uWVI2E6w83D1OvG2aWkEGSsDirzq88FRuR8vC0UUL3DVFSi0dtspnhWbNS2mJ4No3l6zoA1e044tnnO2895m06ETTDdzZJjauMGPcyShoE8y0SrVAswJrdR7spsLeE1XbmcgfUHzRio1rh0qgtE8H4EdiaqSmkDitNcHiIYnbBuWPrPWaMw86BJswPaMjlIq8pJvxxeblVfAf9HE8NLSEFTyD4SAuzTbzWEm1SpdLzyDNIM4K7i4cpQGtlOld6anVgQPPd1cyq4oEweXQ3ja7hOuhHjzHirW9Pj5hm6B3FPJbmMnKm3x2QvkgrXMP5PJS8nm1LxRRZXnE2RvQs24k0azL0tfybmmUGAeCQpvszWhN5cqsLNNN9S6ViS1tPEhlD8xMGIBuYU1zDbZCGvhxtIOVXTV5egDF21mc8CyHRljlktx2ynFK0ptwsLAaBFp8cSyoWS7lHxdzrqwGFxfwekouBYt1G6dZzxp5LZIPfqBiryLykQQLnYaQuzwLzLSs1tKX86uKf8kZAFrQ8iQRJwylVsriXPl2wwUk8YxUqYiu2CnhoUElqO7pPHbOpjm4F4G3COUVKItYXllnCl7MUxYsFj3WFkO7oY5GEG1jKfCirFUts5BuWziZZpgxttFpW6gDgpFN6j1Xrf8fREVJodrR9nai5Khd5ro3f" +
            "vOGXqsX7h9pjHR0hrxIeXIEFvsxJ8WUDGoxpepz5DzNwMRcThxLKIpgq8JhE2zJyZ0mKI5MEoqptS3QN8K72jqwzi4DsjxSr5mzFKePgmJnOyUigYUGxSvxDrHXx9RUpaRl1E1AEdrFgqjRDzlkyvIVQY94Mz5Zehd2pqwtvgG3R3akjhuqlZX0VYRlqvuuaCllNIbx14XbQPeQ001WRmTd3wbX0P3RKcxmKfHfeWaIgHZpgbZnHHukYLCqhXPjDQkAFTrMdsmMAlRlOjVy8nb5aLnlKk900lSD4Szy77IUPaKJvKjoufQ3ufH0RziqkCuxYVLmWu7u9KW8G91wj4OvBevK92jw6xyCWQKC8bS1mXGceWC0027oDII6o5bykUbB09t63AUe9Y4HIuTOY4cr3uGJJJBbfgvDTC2rtbsShRag7E8BikLYAt1zoKyJTCy2ghIgJ5OV7m2INjq23BnkfYsCrcvXuaxt4ULxmOtHi24mvk9v11MI9hKRmt1jYBOKN16XMw6ciN0DPC5Mx1LvUZ8OwNMKGgrJCK1cd86CzguSEO4rDUVkU1C35HLt7desrtmK1V0sJsMmrlcia3hmNiAeaBt1ljWxzqoN2zPLWYHQmmSmQFwNn99PkwGCgrNH1NlJJtZreEUK8gUnnlaOVjazWIJN4Zm197uojuMjxX90GsvTLBBRVlyfMCg4C3L3Ajk8WnLw6ZqZDoJ6UGuy6kcvaVzsa0TlR04k2khSM2ivmRzIv113NoDupHgCN86WLiIHbOsqDFbWT2STjw0HMYWuPYszG9HEbXmL1ZlSO74VeyxxCddpKMpI5FnQcGoeuhCkbsCvCGGeLN9Y6AvqqWqsUPa0rtWMZg2MTFrweDoIvAlg9pgpJYE5v3CQDji6XWxEFdQH7y6qxKY0B8cvhnGKuWRyJsWz3G0CzzbwW5qCRIdFtLT7GNX2KYFajDsfofpQ4RSpqnmvUnUWPOKKtZrIzNVfHnkAjtPO69ziKjPWtnDl2Z";

    protected StorageSegment getFileSegment(int segmentId) {
        StorageSegment segment = null;
        segment = new FileSegment();
        segment.init(this.path, this.name, this.dataFileExt, segmentId, this.initialDataFileSize);
        return segment;
    }

    protected StorageSegment getMemorySegment(int segmentId) {
        StorageSegment segment = null;
        segment = new MemoryMappedSegment();
        segment.init(this.path, this.name, this.dataFileExt, segmentId, this.initialDataFileSize);
        return segment;
    }
}
