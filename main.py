import requests
import json
import re
import time
import os
from datetime import datetime, timedelta
import pandas as pd
# import pyarrow
from typing import List

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False


# 用来把英文和数字翻译成中文存到文件名里。也许还有别的用途。
translation_table = {
    'lastdate': '最后日期',
    'historycurve': '迁徙规模指数',
    'cityrank': '市百分比',
    'provincerank': '省百分比',
    'country': '国家',
    'province': '省',
    'city': '市',
    'move_in': '迁入',
    'move_out': '迁出',

    # 如果你有其他省市数据需要获取，请修改这里
	'110100': '北京市',
	'120100': '天津市',
	'130100': '石家庄市',
	'130200': '唐山市',
	'130300': '秦皇岛市',
	'130400': '邯郸市',
	'130500': '邢台市',
	'130600': '保定市',
	'130700': '张家口市',
	'130800': '承德市',
	'130900': '沧州市',
	'131000': '廊坊市',
	'131100': '衡水市',
	'140100': '太原市',
	'140200': '大同市',
	'140300': '阳泉市',
	'140400': '长治市',
	'140500': '晋城市',
	'140600': '朔州市',
	'140700': '晋中市',
	'140800': '运城市',
	'140900': '忻州市',
	'141000': '临汾市',
	'141100': '吕梁市',
	'150100': '呼和浩特市',
	'150200': '包头市',
	'150300': '乌海市',
	'150400': '赤峰市',
	'150500': '通辽市',
	'150600': '鄂尔多斯市',
	'150700': '呼伦贝尔市',
	'150800': '巴彦淖尔市',
	'150900': '乌兰察布市',
	'152200': '兴安盟',
	'152500': '锡林郭勒盟',
	'152900': '阿拉善盟',
	'210100': '沈阳市',
	'210200': '大连市',
	'210300': '鞍山市',
	'210400': '抚顺市',
	'210500': '本溪市',
	'210600': '丹东市',
	'210700': '锦州市',
	'210800': '营口市',
	'210900': '阜新市',
	'211000': '辽阳市',
	'211100': '盘锦市',
	'211200': '铁岭市',
	'211300': '朝阳市',
	'211400': '葫芦岛市',
	'220100': '长春市',
	'220200': '吉林市',
	'220300': '四平市',
	'220400': '辽源市',
	'220500': '通化市',
	'220600': '白山市',
	'220700': '松原市',
	'220800': '白城市',
	'222400': '延边朝鲜族自治州',
	'230100': '哈尔滨市',
	'230200': '齐齐哈尔市',
	'230300': '鸡西市',
	'230400': '鹤岗市',
	'230500': '双鸭山市',
	'230600': '大庆市',
	'230700': '伊春市',
	'230800': '佳木斯市',
	'230900': '七台河市',
	'231000': '牡丹江市',
	'231100': '黑河市',
	'231200': '绥化市',
	'232700': '大兴安岭地区',
	'310100': '上海市',
	'320100': '南京市',
	'320200': '无锡市',
	'320300': '徐州市',
	'320400': '常州市',
	'320500': '苏州市',
	'320600': '南通市',
	'320700': '连云港市',
	'320800': '淮安市',
	'320900': '盐城市',
	'321000': '扬州市',
	'321100': '镇江市',
	'321200': '泰州市',
	'321300': '宿迁市',
	'330100': '杭州市',
	'330200': '宁波市',
	'330300': '温州市',
	'330400': '嘉兴市',
	'330500': '湖州市',
	'330600': '绍兴市',
	'330700': '金华市',
	'330800': '衢州市',
	'330900': '舟山市',
	'331000': '台州市',
	'331100': '丽水市',
	'340100': '合肥市',
	'340200': '芜湖市',
	'340300': '蚌埠市',
	'340400': '淮南市',
	'340500': '马鞍山市',
	'340600': '淮北市',
	'340700': '铜陵市',
	'340800': '安庆市',
	'341000': '黄山市',
	'341100': '滁州市',
	'341200': '阜阳市',
	'341300': '宿州市',
	'341500': '六安市',
	'341600': '亳州市',
	'341700': '池州市',
	'341800': '宣城市',
	'350100': '福州市',
	'350200': '厦门市',
	'350300': '莆田市',
	'350400': '三明市',
	'350500': '泉州市',
	'350600': '漳州市',
	'350700': '南平市',
	'350800': '龙岩市',
	'350900': '宁德市',
	'360100': '南昌市',
	'360200': '景德镇市',
	'360300': '萍乡市',
	'360400': '九江市',
	'360500': '新余市',
	'360600': '鹰潭市',
	'360700': '赣州市',
	'360800': '吉安市',
	'360900': '宜春市',
	'361000': '抚州市',
	'361100': '上饶市',
	'370100': '济南市',
	'370200': '青岛市',
	'370300': '淄博市',
	'370400': '枣庄市',
	'370500': '东营市',
	'370600': '烟台市',
	'370700': '潍坊市',
	'370800': '济宁市',
	'370900': '泰安市',
	'371000': '威海市',
	'371100': '日照市',
	'371300': '临沂市',
	'371400': '德州市',
	'371500': '聊城市',
	'371600': '滨州市',
	'371700': '菏泽市',
	'410100': '郑州市',
	'410200': '开封市',
	'410300': '洛阳市',
	'410400': '平顶山市',
	'410500': '安阳市',
	'410600': '鹤壁市',
	'410700': '新乡市',
	'410800': '焦作市',
	'410900': '濮阳市',
	'411000': '许昌市',
	'411100': '漯河市',
	'411200': '三门峡市',
	'411300': '南阳市',
	'411400': '商丘市',
	'411500': '信阳市',
	'411600': '周口市',
	'411700': '驻马店市',
	'419001': '济源市',
	'420100': '武汉市',
	'420200': '黄石市',
	'420300': '十堰市',
	'420500': '宜昌市',
	'420600': '襄阳市',
	'420700': '鄂州市',
	'420800': '荆门市',
	'420900': '孝感市',
	'421000': '荆州市',
	'421100': '黄冈市',
	'421200': '咸宁市',
	'421300': '随州市',
	'422800': '恩施土家族苗族自治州',
	'429004': '仙桃市',
	'429005': '潜江市',
	'429006': '天门市',
	'429021': '神农架林区',
	'430100': '长沙市',
	'430200': '株洲市',
	'430300': '湘潭市',
	'430400': '衡阳市',
	'430500': '邵阳市',
	'430600': '岳阳市',
	'430700': '常德市',
	'430800': '张家界市',
	'430900': '益阳市',
	'431000': '郴州市',
	'431100': '永州市',
	'431200': '怀化市',
	'431300': '娄底市',
	'433100': '湘西土家族苗族自治州',
	'440100': '广州市',
	'440200': '韶关市',
	'440300': '深圳市',
	'440400': '珠海市',
	'440500': '汕头市',
	'440600': '佛山市',
	'440700': '江门市',
	'440800': '湛江市',
	'440900': '茂名市',
	'441200': '肇庆市',
	'441300': '惠州市',
	'441400': '梅州市',
	'441500': '汕尾市',
	'441600': '河源市',
	'441700': '阳江市',
	'441800': '清远市',
	'441900': '东莞市',
	'442000': '中山市',
	'445100': '潮州市',
	'445200': '揭阳市',
	'445300': '云浮市',
	'450100': '南宁市',
	'450200': '柳州市',
	'450300': '桂林市',
	'450400': '梧州市',
	'450500': '北海市',
	'450600': '防城港市',
	'450700': '钦州市',
	'450800': '贵港市',
	'450900': '玉林市',
	'451000': '百色市',
	'451100': '贺州市',
	'451200': '河池市',
	'451300': '来宾市',
	'451400': '崇左市',
	'460100': '海口市',
	'460200': '三亚市',
	'460300': '三沙市',
	'460400': '儋州市',
	'469001': '五指山市',
	'469002': '琼海市',
	'469005': '文昌市',
	'469006': '万宁市',
	'469007': '东方市',
	'469021': '定安县',
	'469022': '屯昌县',
	'469023': '澄迈县',
	'469024': '临高县',
	'469025': '白沙黎族自治县',
	'469026': '昌江黎族自治县',
	'469027': '乐东黎族自治县',
	'469028': '陵水黎族自治县',
	'469029': '保亭黎族苗族自治县',
	'469030': '琼中黎族苗族自治县',
	'500100': '重庆市',
	'500200': '重庆市',
	'510100': '成都市',
	'510300': '自贡市',
	'510400': '攀枝花市',
	'510500': '泸州市',
	'510600': '德阳市',
	'510700': '绵阳市',
	'510800': '广元市',
	'510900': '遂宁市',
	'511000': '内江市',
	'511100': '乐山市',
	'511300': '南充市',
	'511400': '眉山市',
	'511500': '宜宾市',
	'511600': '广安市',
	'511700': '达州市',
	'511800': '雅安市',
	'511900': '巴中市',
	'512000': '资阳市',
	'513200': '阿坝藏族羌族自治州',
	'513300': '甘孜藏族自治州',
	'513400': '凉山彝族自治州',
	'520100': '贵阳市',
	'520200': '六盘水市',
	'520300': '遵义市',
	'520400': '安顺市',
	'520500': '毕节市',
	'520600': '铜仁市',
	'522300': '黔西南布依族苗族自治州',
	'522600': '黔东南苗族侗族自治州',
	'522700': '黔南布依族苗族自治州',
	'530100': '昆明市',
	'530300': '曲靖市',
	'530400': '玉溪市',
	'530500': '保山市',
	'530600': '昭通市',
	'530700': '丽江市',
	'530800': '普洱市',
	'530900': '临沧市',
	'532300': '楚雄彝族自治州',
	'532500': '红河哈尼族彝族自治州',
	'532600': '文山壮族苗族自治州',
	'532800': '西双版纳傣族自治州',
	'532900': '大理白族自治州',
	'533100': '德宏傣族景颇族自治州',
	'533300': '怒江傈僳族自治州',
	'533400': '迪庆藏族自治州',
	'540100': '拉萨市',
	'540200': '日喀则市',
	'540300': '昌都市',
	'540400': '林芝市',
	'540500': '山南市',
	'540600': '那曲市',
	'542500': '阿里地区',
	'610100': '西安市',
	'610200': '铜川市',
	'610300': '宝鸡市',
	'610400': '咸阳市',
	'610500': '渭南市',
	'610600': '延安市',
	'610700': '汉中市',
	'610800': '榆林市',
	'610900': '安康市',
	'611000': '商洛市',
	'620100': '兰州市',
	'620200': '嘉峪关市',
	'620300': '金昌市',
	'620400': '白银市',
	'620500': '天水市',
	'620600': '武威市',
	'620700': '张掖市',
	'620800': '平凉市',
	'620900': '酒泉市',
	'621000': '庆阳市',
	'621100': '定西市',
	'621200': '陇南市',
	'622900': '临夏回族自治州',
	'623000': '甘南藏族自治州',
	'630100': '西宁市',
	'630200': '海东市',
	'632200': '海北藏族自治州',
	'632300': '黄南藏族自治州',
	'632500': '海南藏族自治州',
	'632600': '果洛藏族自治州',
	'632700': '玉树藏族自治州',
	'632800': '海西蒙古族藏族自治州',
	'640100': '银川市',
	'640200': '石嘴山市',
	'640300': '吴忠市',
	'640400': '固原市',
	'640500': '中卫市',
	'650100': '乌鲁木齐市',
	'650200': '克拉玛依市',
	'650400': '吐鲁番市',
	'650500': '哈密市',
	'652300': '昌吉回族自治州',
	'652700': '博尔塔拉蒙古自治州',
	'652800': '巴音郭楞蒙古自治州',
	'652900': '阿克苏地区',
	'653000': '克孜勒苏柯尔克孜自治州',
	'653100': '喀什地区',
	'653200': '和田地区',
	'654000': '伊犁哈萨克自治州',
	'654200': '塔城地区',
	'654300': '阿勒泰地区',
	'659001': '石河子市',
	'659002': '阿拉尔市',
	'659003': '图木舒克市',
	'659004': '五家渠市',
	'659005': '北屯市',
	'659006': '铁门关市',
	'659007': '双河市',
	'659008': '可克达拉市',
	'659009': '昆玉市',
	'710100': '台北市',
	'710200': '高雄市',
	'710300': '新北市',
	'710400': '台中市',
	'710500': '台南市',
	'710600': '桃园市',
	'719001': '基隆市',
	'719002': '新竹市',
	'719003': '嘉义市',
	'719004': '新竹县',
	'719005': '宜兰县',
	'719006': '苗栗县',
	'719007': '彰化县',
	'719008': '云林县',
	'719009': '南投县',
	'719010': '嘉义县',
	'719011': '屏东县',
	'719012': '台东县',
	'719013': '花莲县',
	'719014': '澎湖县',
	'810000': '香港',
	'820000': '澳门',
}


class Types:
    """
    存储我们已知和要请求的一些数据。
    """
    # 请不要修改data_type
    data_type = {  # 'lastdate',     # 虽然 lastdate 确实是一个接口，但是我们在每次爬取时
                                     # 只需在最开始调用一次就可以了。所以我们在设计时排除之。
                   # 'historycurve', # 虽然 historycurve 确实是一个接口，但是这个接口会返回
                                     # 所有日期的这个数据。所以只用调用一次就可以了
                 'cityrank',
                 'provincerank'}
    # 请不要修改dt
    dt = {'country',
          'province',
          'city'}
    # 如果你有其他省市数据需要获取，请修改这里
    region = {
        '110100': 'city',
	'120100': 'city',
	'130100': 'city',
	'130200': 'city',
	'130300': 'city',
	'130400': 'city',
	'130500': 'city',
	'130600': 'city',
	'130700': 'city',
	'130800': 'city',
	'130900': 'city',
	'131000': 'city',
	'131100': 'city',
	'140100': 'city',
	'140200': 'city',
	'140300': 'city',
	'140400': 'city',
	'140500': 'city',
	'140600': 'city',
	'140700': 'city',
	'140800': 'city',
	'140900': 'city',
	'141000': 'city',
	'141100': 'city',
	'150100': 'city',
	'150200': 'city',
	'150300': 'city',
	'150400': 'city',
	'150500': 'city',
	'150600': 'city',
	'150700': 'city',
	'150800': 'city',
	'150900': 'city',
	'152200': 'city',
	'152500': 'city',
	'152900': 'city',
	'210100': 'city',
	'210200': 'city',
	'210300': 'city',
	'210400': 'city',
	'210500': 'city',
	'210600': 'city',
	'210700': 'city',
	'210800': 'city',
	'210900': 'city',
	'211000': 'city',
	'211100': 'city',
	'211200': 'city',
	'211300': 'city',
	'211400': 'city',
	'220100': 'city',
	'220200': 'city',
	'220300': 'city',
	'220400': 'city',
	'220500': 'city',
	'220600': 'city',
	'220700': 'city',
	'220800': 'city',
	'222400': 'city',
	'230100': 'city',
	'230200': 'city',
	'230300': 'city',
	'230400': 'city',
	'230500': 'city',
	'230600': 'city',
	'230700': 'city',
	'230800': 'city',
	'230900': 'city',
	'231000': 'city',
	'231100': 'city',
	'231200': 'city',
	'232700': 'city',
	'310100': 'city',
	'320100': 'city',
	'320200': 'city',
	'320300': 'city',
	'320400': 'city',
	'320500': 'city',
	'320600': 'city',
	'320700': 'city',
	'320800': 'city',
	'320900': 'city',
	'321000': 'city',
	'321100': 'city',
	'321200': 'city',
	'321300': 'city',
	'330100': 'city',
	'330200': 'city',
	'330300': 'city',
	'330400': 'city',
	'330500': 'city',
	'330600': 'city',
	'330700': 'city',
	'330800': 'city',
	'330900': 'city',
	'331000': 'city',
	'331100': 'city',
	'340100': 'city',
	'340200': 'city',
	'340300': 'city',
	'340400': 'city',
	'340500': 'city',
	'340600': 'city',
	'340700': 'city',
	'340800': 'city',
	'341000': 'city',
	'341100': 'city',
	'341200': 'city',
	'341300': 'city',
	'341500': 'city',
	'341600': 'city',
	'341700': 'city',
	'341800': 'city',
	'350100': 'city',
	'350200': 'city',
	'350300': 'city',
	'350400': 'city',
	'350500': 'city',
	'350600': 'city',
	'350700': 'city',
	'350800': 'city',
	'350900': 'city',
	'360100': 'city',
	'360200': 'city',
	'360300': 'city',
	'360400': 'city',
	'360500': 'city',
	'360600': 'city',
	'360700': 'city',
	'360800': 'city',
	'360900': 'city',
	'361000': 'city',
	'361100': 'city',
	'370100': 'city',
	'370200': 'city',
	'370300': 'city',
	'370400': 'city',
	'370500': 'city',
	'370600': 'city',
	'370700': 'city',
	'370800': 'city',
	'370900': 'city',
	'371000': 'city',
	'371100': 'city',
	'371300': 'city',
	'371400': 'city',
	'371500': 'city',
	'371600': 'city',
	'371700': 'city',
	'410100': 'city',
	'410200': 'city',
	'410300': 'city',
	'410400': 'city',
	'410500': 'city',
	'410600': 'city',
	'410700': 'city',
	'410800': 'city',
	'410900': 'city',
	'411000': 'city',
	'411100': 'city',
	'411200': 'city',
	'411300': 'city',
	'411400': 'city',
	'411500': 'city',
	'411600': 'city',
	'411700': 'city',
	'419001': 'city',
	'420100': 'city',
	'420200': 'city',
	'420300': 'city',
	'420500': 'city',
	'420600': 'city',
	'420700': 'city',
	'420800': 'city',
	'420900': 'city',
	'421000': 'city',
	'421100': 'city',
	'421200': 'city',
	'421300': 'city',
	'422800': 'city',
	'429004': 'city',
	'429005': 'city',
	'429006': 'city',
	'429021': 'city',
	'430100': 'city',
	'430200': 'city',
	'430300': 'city',
	'430400': 'city',
	'430500': 'city',
	'430600': 'city',
	'430700': 'city',
	'430800': 'city',
	'430900': 'city',
	'431000': 'city',
	'431100': 'city',
	'431200': 'city',
	'431300': 'city',
	'433100': 'city',
	'440100': 'city',
	'440200': 'city',
	'440300': 'city',
	'440400': 'city',
	'440500': 'city',
	'440600': 'city',
	'440700': 'city',
	'440800': 'city',
	'440900': 'city',
	'441200': 'city',
	'441300': 'city',
	'441400': 'city',
	'441500': 'city',
	'441600': 'city',
	'441700': 'city',
	'441800': 'city',
	'441900': 'city',
	'442000': 'city',
	'445100': 'city',
	'445200': 'city',
	'445300': 'city',
	'450100': 'city',
	'450200': 'city',
	'450300': 'city',
	'450400': 'city',
	'450500': 'city',
	'450600': 'city',
	'450700': 'city',
	'450800': 'city',
	'450900': 'city',
	'451000': 'city',
	'451100': 'city',
	'451200': 'city',
	'451300': 'city',
	'451400': 'city',
	'460100': 'city',
	'460200': 'city',
	'460300': 'city',
	'460400': 'city',
	'469001': 'city',
	'469002': 'city',
	'469005': 'city',
	'469006': 'city',
	'469007': 'city',
	'469021': 'city',
	'469022': 'city',
	'469023': 'city',
	'469024': 'city',
	'469025': 'city',
	'469026': 'city',
	'469027': 'city',
	'469028': 'city',
	'469029': 'city',
	'469030': 'city',
	'500100': 'city',
	'500200': 'city',
	'510100': 'city',
	'510300': 'city',
	'510400': 'city',
	'510500': 'city',
	'510600': 'city',
	'510700': 'city',
	'510800': 'city',
	'510900': 'city',
	'511000': 'city',
	'511100': 'city',
	'511300': 'city',
	'511400': 'city',
	'511500': 'city',
	'511600': 'city',
	'511700': 'city',
	'511800': 'city',
	'511900': 'city',
	'512000': 'city',
	'513200': 'city',
	'513300': 'city',
	'513400': 'city',
	'520100': 'city',
	'520200': 'city',
	'520300': 'city',
	'520400': 'city',
	'520500': 'city',
	'520600': 'city',
	'522300': 'city',
	'522600': 'city',
	'522700': 'city',
	'530100': 'city',
	'530300': 'city',
	'530400': 'city',
	'530500': 'city',
	'530600': 'city',
	'530700': 'city',
	'530800': 'city',
	'530900': 'city',
	'532300': 'city',
	'532500': 'city',
	'532600': 'city',
	'532800': 'city',
	'532900': 'city',
	'533100': 'city',
	'533300': 'city',
	'533400': 'city',
	'540100': 'city',
	'540200': 'city',
	'540300': 'city',
	'540400': 'city',
	'540500': 'city',
	'540600': 'city',
	'542500': 'city',
	'610100': 'city',
	'610200': 'city',
	'610300': 'city',
	'610400': 'city',
	'610500': 'city',
	'610600': 'city',
	'610700': 'city',
	'610800': 'city',
	'610900': 'city',
	'611000': 'city',
	'620100': 'city',
	'620200': 'city',
	'620300': 'city',
	'620400': 'city',
	'620500': 'city',
	'620600': 'city',
	'620700': 'city',
	'620800': 'city',
	'620900': 'city',
	'621000': 'city',
	'621100': 'city',
	'621200': 'city',
	'622900': 'city',
	'623000': 'city',
	'630100': 'city',
	'630200': 'city',
	'632200': 'city',
	'632300': 'city',
	'632500': 'city',
	'632600': 'city',
	'632700': 'city',
	'632800': 'city',
	'640100': 'city',
	'640200': 'city',
	'640300': 'city',
	'640400': 'city',
	'640500': 'city',
	'650100': 'city',
	'650200': 'city',
	'650400': 'city',
	'650500': 'city',
	'652300': 'city',
	'652700': 'city',
	'652800': 'city',
	'652900': 'city',
	'653000': 'city',
	'653100': 'city',
	'653200': 'city',
	'654000': 'city',
	'654200': 'city',
	'654300': 'city',
	'659001': 'city',
	'659002': 'city',
	'659003': 'city',
	'659004': 'city',
	'659005': 'city',
	'659006': 'city',
	'659007': 'city',
	'659008': 'city',
	'659009': 'city',
	'710100': 'city',
	'710200': 'city',
	'710300': 'city',
	'710400': 'city',
	'710500': 'city',
	'710600': 'city',
	'719001': 'city',
	'719002': 'city',
	'719003': 'city',
	'719004': 'city',
	'719005': 'city',
	'719006': 'city',
	'719007': 'city',
	'719008': 'city',
	'719009': 'city',
	'719010': 'city',
	'719011': 'city',
	'719012': 'city',
	'719013': 'city',
	'719014': 'city',
	'810000': 'city',
	'820000': 'city',
    }
        
    # 请不要修改move_type
    move_type = {'move_in',
                 'move_out'}


def generate_date_range(start_date_str:"20190201", end_date_str:"20190204"):
    """
    输入两端的日期，输出包括这两天在内的所有日期。

    :param start_date_str: 起始日期，长度为8、格式为年月日的字符串，如"20201231"。
    :param end_date_str: 结束日期，格式同上。
    :return: 一个list，包含从起始日期开始到结束日期为止的所有日期，格式同上。
    """
    # 将输入的日期字符串转换为datetime对象
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')

    # 初始化日期列表，包含起始日期
    date_list = [start_date.strftime('%Y%m%d')]

    # 生成日期范围
    current_date = start_date
    while current_date < end_date:
        current_date += timedelta(days=1)
        date_list.append(current_date.strftime('%Y%m%d'))

    return date_list


def get_timestamp():
    """
    获得当前时间戳。

    :return: 长度为13的时间戳
    """
    return str(int(time.time() * 1000))  # * 1000获取毫秒级时间戳


def get_lastdate():
    """
    从API获得目前API有的最晚的日期。

    :return: 长度为8、格式为年月日的字符串，如"20201231"。
    """
    url = f'http://huiyan.baidu.com/migration/lastdate.jsonp'
    response = requests.get(url)
    json_data_match = re.search(r'{.*}', response.text)
    if json_data_match:
        json_data_str = json_data_match.group()

        # Decode JSON string, automatically handling Unicode characters
        json_data = json.loads(json_data_str)
        return_value = json_data['data']['lastdate']
        print(f"成功获取到API最晚日期：{return_value}")
        return return_value
    else:
        print('获取API最晚日期时失败')


def get_historycurve(region:'420100', move_type:'move_out'):
    """
    从API获得一个区域（省或市）的迁入或迁出的迁徙规模指数。historycurve API会直接返回所有日期。存储文件到文件名。

    :param region: 行政区域编码，如300001。
    :param move_type: 'move_in' 或 'move_out'
    :return: 成功返回 True，失败返回 False
    """
    # 注意：这个文件要覆写，因为每次抓到的可能不一样。不需要判断是否已存在文件。
    output_file = f'./data/{translation_table[region]}_{translation_table[move_type]}_{translation_table["historycurve"]}.json'

    print(f"正在获取"
          f' {translation_table[region]}'
          f' {translation_table[move_type]} 的'
          f' {translation_table["historycurve"]}')

    url = f"http://huiyan.baidu.com/migration/historycurve.jsonp?dt={Types.region[region]}&id={region}&type={move_type}"
    response = requests.get(url)
    json_data_match = re.search(r'{.*}', response.text)
    if json_data_match:
        json_data_str = json_data_match.group()

        # Decode JSON string, automatically handling Unicode characters
        json_data = json.loads(json_data_str)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        print('成功')
        return True
    else:
        print('失败')
        return False


# http://huiyan.baidu.com/migration/cityrank.jsonp?
# dt=country&id=0&type=move_in&date=20240130&callback=jsonp_1706674779082_8488633
def download_and_convert_jsonp(data_type: 'cityrank', dt: 'city', id: '420100', move_type: 'move_out', date: '20190201', callback: 'jsonp_1234567890123'):
    """
    从百度慧眼的API上获得数据并存到本地。

    :param data_type: 就是你要获取的信息类型，有且仅有下列几个值：
        lastdate，返回最后有数据的日期（昨天）；
        historycurve，返回从20190112到lastdate为止的迁徙规模指数；
        cityrank，返回迁入xx来源地（城市级别）；
        provincerank，返回xx来源地（省份级别）。
    :param dt: 级别。可选的值有：country，province，city。
    :param id: 哪里的数据（若dt为国家则不需要id）。参照"行政区划乡镇清单201910"表。级别要和上面的id对应。
    :param move_type: 迁入还是迁出。可选的值有：move_in, move_out。
    :param date: 要的日期，格式：八位数的年月日，如20240131。data_type为lastdate或historycurve时不需要这个参数。
    :param callback: 没什么用，只是为了确认返回值是我这次要的数据罢了。给个时间戳就行。
    :param output_file: 该文件要存到的文件名。
    :return: boolean 成功返回True，失败返回False
    """
    # TODO: 如果要爬大量数据，搞多线程
    output_file = f'./data/{translation_table.get(id, id)}_' \
                  f'{translation_table.get(move_type, move_type)}_' \
                  f'{translation_table.get(data_type, data_type)}_' \
                  f'{date}.json'

    # Construct the full URL with query parameters
    url = f'http://huiyan.baidu.com/migration/{data_type}.jsonp?dt={dt}&id={id}&type={move_type}&date={date}&callback={callback}'

    response = requests.get(url)

    # Extract JSON data part using regular expression
    json_data_match = re.search(r'{.*}', response.text)
    if json_data_match:
        json_data_str = json_data_match.group()

        # Decode JSON string, automatically handling Unicode characters
        json_data = json.loads(json_data_str)

        # 如果要导出 CSV 文件，可以在这里操作json_data。可以修改下面的代码：
        # if json_data['errmsg'] == 'SUCCESS':
        #     data = json_data['data']['list']
        #     file = pd.DataFrame({id: data}).T
        #     csv_filename = f'./data/{translation_table[id]}_' \
        #                    f'{translation_table[move_type]}_' \
        #                    f'{translation_table[data_type]}_' \
        #                    f'{date}.csv'
        #     file.to_csv(csv_filename, encoding='utf-8')

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        print('成功')
        return json_data
    else:
        print('失败')
        return None


def get_data(region: '420100', data_type: 'cityrank', move_type: 'move_out', date: '20190201'):
    timestamp = get_timestamp()
    print(f"正在获取"
          f' {translation_table[region]}'
          f' {translation_table[move_type]} 的'
          f' {translation_table[data_type]}，日期为{date}')
    return download_and_convert_jsonp(
        data_type=data_type,
        dt=Types.region[region],
        id=region,
        move_type=move_type,
        date=date,
        callback=timestamp
    )


def get_by_date(from_date, to_date, lastdate):
    # 将日期字符串转换为datetime对象
    from_date_dt = datetime.strptime(from_date, '%Y%m%d')
    to_date_dt = datetime.strptime(to_date, '%Y%m%d')
    lastdate_dt = datetime.strptime(lastdate, '%Y%m%d')

    print("正在检查日期输入合法性")

    # 检查from_date是否小于'20190112'
    earlydate = datetime(2019, 1, 12)
    if from_date_dt < earlydate:
        print("你的起始日期比API目前有的最早日期还早。已经将起始日期设置成API的最早日期，并尝试继续运行。")
        from_date_dt = earlydate

    # 检查to_date是否大于lastdate
    if to_date_dt > lastdate_dt:
        print("你的结束日期比API目前有的最晚日期还晚。已经将结束日期设置成API的最晚日期，并尝试继续运行。")
        to_date_dt = lastdate_dt

    # 检查输入合法性
    if from_date_dt > to_date_dt:
        print("你的起始日期需要比结束日期早。")
        return

    print("成功")

    # 转换为字符串，以便生成日期范围
    from_date = from_date_dt.strftime('%Y%m%d')
    to_date = to_date_dt.strftime('%Y%m%d')

    for region in Types.region.keys():
        for move_type in Types.move_type:
            get_historycurve(region, move_type)  # 获取他的迁徙规模指数

    for date in generate_date_range(from_date, to_date):  # 日期
        for region in Types.region.keys():
            for move_type in Types.move_type:
                for data_type in Types.data_type:
                    get_data(region, data_type, move_type, date)
    print("完成")



