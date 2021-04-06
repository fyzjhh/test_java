package com.test.algorithm;

public class TigerMachine {
	
	
	 //初始化玩牌游戏
    public void PlayCardsInit()
    {
        if (iMaskedCardsType[0] == 0)//服务器从新启动后的初始化
        {
           
            iMaskedCardsType[0] = -1;
            iMaskedCardsType[Convert.ToInt16(Single)] = 1;
            iMaskedCardsType[Convert.ToInt16(Pair)] = 1;
            iMaskedCardsType[Convert.ToInt16(TwoPair)] = 0;
            iMaskedCardsType[Convert.ToInt16(Straight)] = 0;
            iMaskedCardsType[Convert.ToInt16(Flush)] = 0;
            iMaskedCardsType[Convert.ToInt16(ThreeOfAKind)] = 0;
            iMaskedCardsType[Convert.ToInt16(FullHouse)] = 0;
            iMaskedCardsType[Convert.ToInt16(FourOfAKind)] = 0;
            iMaskedCardsType[Convert.ToInt16(StraightFlush)] = 0;
        }
    }
    //检查出触发时间
    public void CheckTrigger()
    {
        String sDateAndTimeBuf = System.DateTime.Now.Date.ToString("yyyy-MM-dd");//系统当前日期
        String[] sSplitBufDate = sDateAndTimeBuf.split(' ');//去空--------------------------------------------系统时间里有空格么？

        String[] sSplitBufTime = System.DateTime.Now.TimeOfDay.ToString().split(':');//取出今天的时间并分割,精确到小数点后面六位

        //此代码一天执行一次
        if (sSplitBufDate[0] != sDateBuf)//缓存日期不是今天日期，即今天还没有执行过此代码
        {
            sDateBuf = sSplitBufDate[0];//缓存日期=今天日期，即标记今天此代码已经被执行过
            if (sSplitBufTime[0] == iOnTimeTrigger8.ToString())//每晚8点触发三张加对子(概率，下同)
            {
              
                iMaskedCardsType[Convert.ToInt16(FullHouse)] = 1;//触发加对子
            }
        }

        //此代码一小时执行一次
        if (sSplitBufTime[0] != sHourBuf)//另一个小时开始
        {
            sHourBuf = sSplitBufTime[0];//缓存小时数=当前小时数
            if (--ThreeOfAKindCount <= 0)//一个小时倒计时结束
            {
                ThreeOfAKindCount = iHoursPerTrigger;//ThreeOfAKiindCount=2
                iMaskedCardsType[Convert.ToInt16(ThreeOfAKind)]++;//每2小时触发三张
            }
        }

        //此代码一分钟执行一次
        if (sSplitBufTime[1] != sMinuteBuf)//另一分钟开始
        {
            sMinuteBuf = sSplitBufTime[1];//缓存分钟数=当前分钟数
            if (--FlushCount <= 0)//30分钟倒计时结束
            {
                FlushCount = iMinutesPerTrigger30;//FlushCount=30
                iMaskedCardsType[Convert.ToInt16(Flush)]++;//每30分钟触发同花
            }
            if (--StraightCount <= 0)//5分钟倒计时结束
            {
                StraightCount = iMinutesPerTrigger5;//StraightCount=5
                iMaskedCardsType[Convert.ToInt16(Straight)]++;//每5钟触发顺子
            }
            if (--TwoPairCount <= 0)//一分钟倒计时结束
            {
                TwoPairCount = iMinutesPerTrigger1;//TwoPairCount=1
                iMaskedCardsType[Convert.ToInt16(TwoPair)]++;//每1分钟触发两对半
            }
        }
    }
    //更新触发时间
    public void UpdateTrigger(String sCurrentCards)
    {
        String sCurrentHand = GetCardsType(sCurrentCards);//返回 牌类型,牌1数字~牌2数字~牌3数字~牌4数字~牌5数字
        String[] sSplitBuf = sCurrentHand.split(',');
        if (sSplitBuf[0] != Single && sSplitBuf[0] != Pair)//牌类型不是单张或者对子
            iMaskedCardsType[Convert.ToInt16(sSplitBuf[0])]--;//重新将其他牌类型初始化为0
    }
    //返回：计算机发的5张牌。例：牌1的ImageUrl~牌2的ImageUrl~牌3的ImageUrl~牌4的ImageUrl~牌5的ImageUrl。
    public String GetNewHand()
    {
        String sNewHand = "";

        while (sNewHand == "")
        {
            Random NewType = new Random();
            Random NewNumber = new Random();

            int[] iCardsType = new int[iNumOfCards];
            int[] iCardsNumber = new int[iNumOfCards];

            //牌的类型
            iCardsType[0] = NewType.Next(1, 5);//1-5之间的一个随机数
            iCardsType[1] = NewType.Next(1, 5);
            iCardsType[2] = NewType.Next(1, 5);
            iCardsType[3] = NewType.Next(1, 5);
            iCardsType[4] = NewType.Next(1, 5);
           
            //牌号
            iCardsNumber[0] = NewNumber.Next(1, 14);//1-14之间的随机数
            iCardsNumber[1] = NewNumber.Next(1, 14);
            iCardsNumber[2] = NewNumber.Next(1, 14);
            iCardsNumber[3] = NewNumber.Next(1, 14);
            iCardsNumber[4] = NewNumber.Next(1, 14);

            sNewHand = CheckNewHand(iCardsType, iCardsNumber);//组合五张牌成一个字符串  花色_牌号~花色_牌号……
        }
        return sNewHand;
    }
    //返回5张牌如2_3~2_3~2_3~2_3~2_3
    public String CheckNewHand(int [] iCardType, int[] iCardsNumber)
    {
        if (lxFile_String.CheckDepurecatedNum(iCardsNumber))//几张牌的数字要完全不相同
            return "";
        else if (CheckDepurecatedCardType(iCardType))
        {
            return iCardType[0].ToString() + "_" + iCardsNumber[0].ToString() + "~" +
                   iCardType[1].ToString() + "_" + iCardsNumber[1].ToString() + "~" +
                   iCardType[2].ToString() + "_" + iCardsNumber[2].ToString() + "~" +
                   iCardType[3].ToString() + "_" + iCardsNumber[3].ToString() + "~" +
                   iCardType[4].ToString() + "_" + iCardsNumber[4].ToString();
        }
        return "";
    }
    //检查牌的类型是否相同,五张牌的类型完全不相同return true ，完全相同 return false
    public bool CheckDepurecatedCardType(int[] iCardType)
    {
        if (iCardType[0] != iCardType[1] && iCardType[0] != iCardType[2] && iCardType[0] != iCardType[3] &&
            iCardType[1] != iCardType[2] && iCardType[1] != iCardType[3] && iCardType[2] != iCardType[1] && iCardType[2] != iCardType[3])//牌的类型完全不相同
            return true;
        else
            return false;
    }

    //返回：计算机发的1张新牌。例：新牌的ImageUrl,新的牌要求和原来的牌不能相同
    //并且如果任意替换原来牌中的其中一张,组合成的牌的类型会出现iMaskedCardsType[牌的类型] = 0
    public String GetNewCards(String sCurrentHand)
    {
        String sNewCards = "";
        String[] sSplitBuf = sCurrentHand.split('~');//将不同的牌分割开

        CheckTrigger();//检察出触发时间
        Random NewType = new Random();
        Random NewNumber = new Random();

        bool bFlag = true;
        while (bFlag == true)
        {
            int iCardsType = NewType.Next(1, 5);//花色
            int iCardsNumber = NewNumber.Next(1, 14);//号码

            sNewCards = iCardsType.ToString() + "_" + iCardsNumber.ToString();//新牌

            if (lxFile_String.StringSearch(sExistedCards, sNewCards) == -1)//搜索相同的牌
            {
                if (++iExistedCardsIndex >= iMaxExistedCardsBuf)//如果越界
                    iExistedCardsIndex = 0;

                sExistedCards[iExistedCardsIndex] = sNewCards;//把新的牌放入存在的牌中

                bFlag = lxFile_String.CheckSameString(sSplitBuf, sNewCards);//检查现在的牌是否和新牌是否相同
                if (bFlag == false)//产生一张和现有牌不相同的牌
                    bFlag = CheckMaskedCardType(sNewCards, sCurrentHand);
            }
        }
        return sNewCards;
    }
    //把原有的牌逐个用新增的一张牌替换，并判断替换后的牌的类型（iCardType），判断是否存在iMaskedCardsType[iCardType] == 0的情况
    //如果存在return true 如果不存在return false
    public bool CheckMaskedCardType(String sNewCards, String sCurrentHand)
    {
        int iMasked = 0;
        for (int i = 0; i < iNumOfCards; i++)//iNumOfCards牌数,=5
        {
            String[] sSplitBuf = sCurrentHand.split('~');//将不同的牌分割开
            sSplitBuf[i] = sNewCards;
            String sCurrentCardsBuf = sSplitBuf[0] + "~" + sSplitBuf[1] + "~" + sSplitBuf[2] + "~" + sSplitBuf[3] + "~" + sSplitBuf[4];//重新组合
            String sCardType = GetCardsType(sCurrentCardsBuf);//返回:牌类型,牌1数字~牌2数字~牌3数字~牌4数字~牌5数字,""
            String[] sSplitBuf1 = sCardType.split(',');
            int iCardType = Convert.ToInt16(sSplitBuf1[0]);
            if (iMaskedCardsType[iCardType] == 0)
                iMasked++;
        }
        if (iMasked == 0)
            return false;
        else
            return true;
    }

    //返回：换牌后的新5张牌。    例：牌1的ImageUrl~牌2的ImageUrl~牌3的ImageUrl~牌4的ImageUrl~牌5的ImageUrl。
    //输入：当前5张牌的ImageUrl。例：牌1的ImageUrl~牌2的ImageUrl~牌3的ImageUrl~牌4的ImageUrl~牌5的ImageUrl。
    //     sNewCardsImage要加入的一张新牌   iPosition要替换的位置
    public String ChangeCards(String sCurrentCardsInHand, String sNewCardsImage, int iPosition)
    {
        String[] sSplitBuf = sCurrentCardsInHand.split('~');
        sSplitBuf[iPosition] = sNewCardsImage;
        return sSplitBuf[0] + "~" + sSplitBuf[1] + "~" + sSplitBuf[2] + "~" + sSplitBuf[3] + "~" + sSplitBuf[4];//重新组合
    }
    //花色1是黑桃，花色2是红桃，花色3是梅花，花色4是方块
    public String GetCardsImages(int iCardsType, int iCardsNumber)
    {
        return lxAppSettings.sGetAppSettings("PICTURE_DIR") + iCardsType.ToString() + "_" + lxAppSettings.sGetAppSettings("PICTURE_DIR") + iCardsNumber.ToString();
    }

    //返回值是牌大小的编码:N,M_0_P_Q_R，N表示牌的类型,M、O、P、Q、R表示在同类中的大小顺序(数越大越好)，M、O、P、Q、R排列时大的排在前面
    //例：红桃J，方块7，黑桃7，黑桃J，草花6的排列是3(二对半)，3,11~11~7~7~6（JJ776）

    //判断是否是同花顺
    //输入:输入:sCurrentCards  牌1的ImageUrl~牌2的ImageUrl~牌3的ImageUrl~牌4的ImageUrl~牌5的ImageUrl
    //输出:"" = 不是同花顺
    //String = "StraightFlush,牌号1~牌号2~牌号3~牌号4~牌号5"(牌号1-5从大到小排列)
    public String CheckStraightFlush(String sCurrentCards)//同花顺=9
    {
        String sBuf1 = "";
        String sBuf2 = "";

        if ((sBuf1 = CheckFlush(sCurrentCards)) != "" && (sBuf2 = CheckStraight(sCurrentCards)) != "")
        {
            String[] sSplitBuf = sBuf2.split(',');
            return StraightFlush + "," + sSplitBuf[1];
        }
        else
            return "";
    }
    public String CheckFourOfAKind(String sCurrentCards)//四张相同=8
    {
        String[] sCards = GetCards(sCurrentCards);
        String[] sSameCardsBuf = new String[4];
        String sOther = "";

        for (int k = 0; k < iNumOfCards; k++)
        {
            int iNumOfSame = 0;
            for (int i = 0; i < iNumOfCards; i++)
            {
                if (sCards[iNumOfCards] == sCards[i + iNumOfCards])
                {
                    sSameCardsBuf[iNumOfSame++] = sCards[i + iNumOfCards];
                }
                else
                    sOther = sCards[i + iNumOfCards];
            }
            if (iNumOfSame == 4)
            {
                return FourOfAKind + "," + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sSameCardsBuf[2] + "~" + sSameCardsBuf[3] + "~" + sOther;
            }
        }

        return "";
    }
    /*
     * 
     * 
     **/
    public String CheckFullHouse(String sCurrentCards)//三张加对子=7
    {
        String sBuf = CheckThreeOfAKind(sCurrentCards);
        if (sBuf != "")
        {
            String[] sSplitBuf1 = sBuf.split('~');
            if (sSplitBuf1[3] == sSplitBuf1[4])
            {
                String[] sSplitBuf2 = sSplitBuf1[0].split(',');
                return FullHouse + "," + sSplitBuf2[1] + "~" + sSplitBuf1[1] + "~" + sSplitBuf1[2] + "~" + sSplitBuf1[3] + "~" + sSplitBuf1[4];
            }
            else
                return "";
        }
        return "";
    }
    //

    public String CheckThreeOfAKind(String sCurrentCards)//三张=6
    {
        String[] sCards = GetCards(sCurrentCards);
        String[] sSameCardsBuf = new String[5];
        String[] sOther = new String[5];

        for (int k = 0; k < iNumOfCards; k++)
        {
            int iNumOfSame = 0;
            int j = 0;
            for (int i = 0; i < iNumOfCards; i++)
            {
                if (sCards[k + iNumOfCards] == sCards[i + iNumOfCards])
                {
                    sSameCardsBuf[iNumOfSame++] = sCards[i + iNumOfCards];
                }
                else
                    sOther[j++] = sCards[i + iNumOfCards];
            }
            if (iNumOfSame == 3)
            {
                if (Convert.ToInt16(sOther[0]) > Convert.ToInt16(sOther[1]))
                {
                    sSameCardsBuf[3] = sOther[0];
                    sSameCardsBuf[4] = sOther[1];
                }
                else
                {
                    sSameCardsBuf[4] = sOther[0];
                    sSameCardsBuf[3] = sOther[1];
                }
                return ThreeOfAKind + "," + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sSameCardsBuf[2] + "~" + sSameCardsBuf[3] + "~" + sSameCardsBuf[4];
            }
        }
        return "";
    }
    /* 
       作用:判断是否是同花
       输入:sCurrentCards  牌1的ImageUrl~牌2的ImageUrl~牌3的ImageUrl~牌4的ImageUrl~牌5的ImageUrl
       输出:"" = 不是同花
            String = "Flush,牌号1~牌号2~牌号3~牌号4~牌号5"(牌号1-5从大到小排列)
    */

    public String CheckFlush(String sCurrentCards)//同花=5
    {
        String[] sCards = GetCards(sCurrentCards);//返回：[0]-[4]放花色，[5]-[9]数字

        if (sCards[0] == sCards[1] && sCards[0] == sCards[2] && sCards[0] == sCards[3] && sCards[0] == sCards[4])//同花
        {
            String[] sCardsAfterSort = SortNumbers(sCards, iNumOfCards);//牌号,从大到小排列
            return Flush + "," + sCardsAfterSort[0] + "~" + sCardsAfterSort[1] + "~" + sCardsAfterSort[2] + "~" + sCardsAfterSort[3] + "~" + sCardsAfterSort[4];
        }
        else
            return "";//表示不是Flash
    }
    /* 
      作用:判断是否是顺子
      输入:sCurrentCards  牌1的ImageUrl~牌2的ImageUrl~牌3的ImageUrl~牌4的ImageUrl~牌5的ImageUrl
      输出:"" = 不是顺子
           String = "Straight,牌号1~牌号2~牌号3~牌号4~牌号5"(牌号1-5从大到小排列)
   */
    public String CheckStraight(String sCurrentCards)//顺子=4
    {
        String[] sCards = GetCards(sCurrentCards);
        String[] sCardsAfterSort = SortNumbers(sCards, iNumOfCards);

        for (int i = 0; i < sCardsAfterSort.Length - 1; i++)
        {
            if (Convert.ToInt16(sCardsAfterSort[i]) - 1 != Convert.ToInt16(sCardsAfterSort[i + 1]))
                return "";//表示不是Stright
        }
        return Straight + "," + sCardsAfterSort[0] + "~" + sCardsAfterSort[1] + "~" + sCardsAfterSort[2] + "~" + sCardsAfterSort[3] + "~" + sCardsAfterSort[4];
    }
    public String CheckTwoPair(String sCurrentCards)//二对半=3
    {
        String[] sCards = GetCards(sCurrentCards);
        String[] sSameCardsBuf = new String[5];
        String[] sOther = new String[5];

        for (int k = 0; k < iNumOfCards; k++)
        {
            int iNumOfSame = 0;
            int j = 0;
            for (int i = 0; i < iNumOfCards; i++)
            {
                if (sCards[k + iNumOfCards] == sCards[i + iNumOfCards])
                {
                    sSameCardsBuf[iNumOfSame++] = sCards[i + iNumOfCards];
                }
                else
                    sOther[j++] = sCards[i + iNumOfCards];
            }
            if (iNumOfSame == 2)
            {
                if (sOther[0] == sOther[1])//如果出现sOther[0] == sOther[1 == sOther[2]即三张加对子怎么办?
                {
                    if (Convert.ToInt16(sSameCardsBuf[0]) >= Convert.ToInt16(sOther[0]))
                        return TwoPair + "," + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sOther[0] + "~" + sOther[1] + "~" + sOther[2];
                    else
                        return TwoPair + "," + sOther[0] + "~" + sOther[1] + "~" + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sOther[2];
                }
                else if (sOther[0] == sOther[2])
                {
                    if (Convert.ToInt16(sSameCardsBuf[0]) >= Convert.ToInt16(sOther[0]))
                        return TwoPair + "," + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sOther[0] + "~" + sOther[2] + "~" + sOther[1];
                    else
                        return TwoPair + "," + sOther[0] + "~" + sOther[2] + "~" + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sOther[1];
                }
                else if (sOther[1] == sOther[2])
                {
                    if (Convert.ToInt16(sSameCardsBuf[0]) >= Convert.ToInt16(sOther[0]))
                        return TwoPair + "," + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sOther[1] + "~" + sOther[2] + "~" + sOther[0];
                    else
                        return TwoPair + "," + sOther[1] + "~" + sOther[2] + "~" + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sOther[0];
                }
                else
                {
                }
            }
        }
        return "";
    }
    public String CheckPair(String sCurrentCards)//对子=2
    {
        String[] sCards = GetCards(sCurrentCards);
        String[] sSameCardsBuf = new String[5];
        String[] sOther = new String[5];

        for (int k = 0; k < iNumOfCards; k++)
        {
            int iNumOfSame = 0;
            int j = 0;
            for (int i = 0; i < iNumOfCards; i++)
            {
                if (sCards[k + iNumOfCards] == sCards[i + iNumOfCards])
                {
                    sSameCardsBuf[iNumOfSame++] = sCards[i + iNumOfCards];
                }
                else
                    sOther[j++] = sCards[i + iNumOfCards];
            }
            if (iNumOfSame == 2)
            {
                String[] sSortBuf = SortNumbers(sOther, 0);
                return Pair + "," + sSameCardsBuf[0] + "~" + sSameCardsBuf[1] + "~" + sSortBuf[0] + "~" + sSortBuf[1] + "~" + sSortBuf[2];
            }
        }
        return "";
    }
    public String CheckSingle(String sCurrentCards)//单张=1
    {
        if (CheckFlush(sCurrentCards) == "")
        {
            String[] sCards = GetCards(sCurrentCards);
            String[] sSortBuf = SortNumbers(sCards, iNumOfCards);
            return Single + "," + sSortBuf[0] + "~" + sSortBuf[1] + "~" + sSortBuf[2] + "~" + sSortBuf[3] + "~" + sSortBuf[4];
        }
        return "";
    }
    //输入：花色_数字~花色_数字~花色_数字~花色_数字~花色_数字
    //返回：[0]-[4]放花色，[5]-[9]数字
    public String[] GetCards(String sCurrentCards)
    {
        String[] sCards = new String[10];
        String[] sSplitBuf = sCurrentCards.split('~');

        for (int i = 0; i < sSplitBuf.Length; i++)
        {
            String[] sCards1 = sSplitBuf[i].split('_');
            sCards[i] = sCards1[0];//花色
            sCards[i + iNumOfCards] = sCards1[1];//数字
        }
        return sCards;
    }
    //输入：花色_数字~花色_数字~花色_数字~花色_数字~花色_数字
    //返回：牌类型,牌1数字~牌2数字~牌3数字~牌4数字~牌5数字,""表示没有找到相应的类型
    public String GetCardsType(String sCurrentCards)
    {
        String sCardsType = "";
        if ((sCardsType = CheckStraightFlush(sCurrentCards)) == "")
            if ((sCardsType = CheckFourOfAKind(sCurrentCards)) == "")
                if ((sCardsType = CheckFullHouse(sCurrentCards)) == "")
                    if ((sCardsType = CheckThreeOfAKind(sCurrentCards)) == "")
                        if ((sCardsType = CheckFlush(sCurrentCards)) == "")
                            if ((sCardsType = CheckStraight(sCurrentCards)) == "")
                                if ((sCardsType = CheckTwoPair(sCurrentCards)) == "")
                                    if ((sCardsType = CheckPair(sCurrentCards)) == "")
                                        if ((sCardsType = CheckSingle(sCurrentCards)) == "")
                                            return "";
        return sCardsType;
    }
    //如果sCurrentCards > sCurrentCards2 则返回1,反之返回0，如果找不到相应的类型返回-1,相等返回2
    public int CompareCards(String sCurrentCards1, String sCurrentCards2)
    {
        String CardsType1 = GetCardsType(sCurrentCards1);
        String CardsType2 = GetCardsType(sCurrentCards2);
        if (CardsType1 != "" && CardsType2 != "")
        {
            //分割类型
            String[] sCardTypeIndex1 = CardsType1.split(',');
            String[] sCardTypeIndex2 = CardsType2.split(',');
            String[] sCardNumber1 = sCardTypeIndex1[1].split('~');
            String[] sCardNumber2 = sCardTypeIndex2[1].split('~');

            if (Convert.ToInt16(sCardTypeIndex1[0]) > Convert.ToInt16(sCardTypeIndex2[0]))//比较
                return 1;
            else if (Convert.ToInt16(sCardTypeIndex1[0]) < Convert.ToInt16(sCardTypeIndex2[0]))
                return 0;
            else
            {
                for (int i = 0; i < sCardNumber1.Length; i++)
                {
                    if (Convert.ToInt16(sCardNumber1[i]) > Convert.ToInt16(sCardNumber2[i]))
                        return 1;
                    else if (Convert.ToInt16(sCardNumber1[i]) < Convert.ToInt16(sCardNumber2[i]))
                        return 0;
                }
                return 2;
            }
        }
        return -1;
    }
    //排序
    //输入:sNumberToSout:[0]-[4]放花色，[5]-[9]数字   iOffset:牌数
    //输出:String[],只有牌号 从大到小排列
    public String[] SortNumbers(String[] sNumberToSout, int iOffset)
    {
        int iNumToSort = sNumberToSout.Length - iOffset;

        String[] sCompareResult = new String[iNumToSort];
        String[] sNumBuf = new String[iNumToSort];//保存号码

        for (int n = 0; n < iNumToSort; n++)
            sNumBuf[n] = sNumberToSout[n + iOffset];

        for (int k = 0; k < iNumToSort; k++)
        {
            int j = 0;
            String sMaxBuf = "0";
            for (int i = 0; i < iNumToSort; i++)
            {
                if (Convert.ToInt16(sNumBuf[i]) > Convert.ToInt16(sMaxBuf))
                {
                    sMaxBuf = sNumBuf[i];
                    j = i;
                }
            }
            sCompareResult[k] = sNumBuf[j];//当前最大值
            sNumBuf[j] = "0";//将最大值清零
        }
        return sCompareResult;
    }

    public String OpenLottery(String sCardsType)//2对半=1个金币，顺子=2个，同花=5个，3张=20个，3对半=100个，4张 = 500个（最高），同花顺 = 1000个（最高）
    {
        String[] sSplitBuf = sCardsType.split(',');//分割
        switch (sSplitBuf[0])
        {
            case "3":
                return "1";
            case "4":
                return "2";
            case "5":
                return "5";
            case "6":
                return "20";
            case "7":
                return "100";
            case "8":
                return OpenLotteryFourOfAKind(sSplitBuf[1]);//四张相同
            case "9":
                return OpenLotteryStraightFlush(sSplitBuf[1]);//同花顺
            default:
                return "";
        }
    }

    public String OpenLotteryFourOfAKind(String sCards)//4张13=500个金币，4张11 = 475个金币，...4张1 = 200个金币
    {
        String[] sSplitBuf = sCards.split('~');//分割
        String sCardNumber = "";
        for (int i = 0; i < sSplitBuf.Length; i++)
        {
            if (sSplitBuf[i] == sSplitBuf[i + 1])//如果相同
            {
                sCardNumber = sSplitBuf[i];
                break;
            }
        }
        switch (sCardNumber)
        {
            case "13":
                return "500";
            case "12":
                return "475";
            case "11":
                return "450";
            case "10":
                return "425";
            case "9":
                return "400";
            case "8":
                return "375";
            case "7":
                return "350";
            case "6":
                return "325";
            case "5":
                return "300";
            case "4":
                return "275";
            case "3":
                return "250";
            case "2":
                return "225";
            case "1":
                return "200";
            default:
                return "";
        }
    }

    public String OpenLotteryStraightFlush(String sCards)
    {
        String[] sSplitBuf = sCards.split('~');
        String sSortNum = lxFile_String.FindSmallestNum(sSplitBuf);//查找最小的数字
        switch (sSortNum)
        {
            case "9":
                return "1000";
            case "8":
                return "950";
            case "7":
                return "900";
            case "6":
                return "850";
            case "5":
                return "800";
            case "4":
                return "750";
            case "3":
                return "700";
            case "2":
                return "650";
            case "1":
                return "600";
            default:
                return "";
        }
    }

  
}
