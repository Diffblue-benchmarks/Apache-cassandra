/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.obs;

import org.apache.cassandra.utils.obs.BitUtil;
import org.junit.Assert;
import org.junit.Test;

public class BitUtilTest {

  @Test
  public void testIntIsPowerOfTwo() {
    Assert.assertTrue(BitUtil.isPowerOfTwo(2));
    Assert.assertFalse(BitUtil.isPowerOfTwo(3));
  }

  @Test
  public void testLongIsPowerOfTwo() {
    Assert.assertTrue(BitUtil.isPowerOfTwo(2L));
    Assert.assertFalse(BitUtil.isPowerOfTwo(3L));
  }

  @Test
  public void testLongNextHighestPowerOfTwo() {
    Assert.assertEquals(0L, BitUtil.nextHighestPowerOfTwo(0L));
    Assert.assertEquals(4L, BitUtil.nextHighestPowerOfTwo(3L));
  }

  @Test
  public void testIntNextHighestPowerOfTwo() {
    Assert.assertEquals(0, BitUtil.nextHighestPowerOfTwo(0));
    Assert.assertEquals(4, BitUtil.nextHighestPowerOfTwo(3));
  }

  @Test
  public void testNtz2() {
    Assert.assertEquals(16, BitUtil.ntz2(124_555_952_128L));
    Assert.assertEquals(8, BitUtil.ntz2(124_555_959_552L));
    Assert.assertEquals(32, BitUtil.ntz2(124_554_051_584L));
    Assert.assertEquals(0, BitUtil.ntz2(1_376_285L));
  }

  @Test
  public void testNtz3() {
    Assert.assertEquals(3, BitUtil.ntz3(8L));
    Assert.assertEquals(11, BitUtil.ntz3(2048L));
    Assert.assertEquals(14, BitUtil.ntz3(49_152L));
    Assert.assertEquals(30, BitUtil.ntz3(1_073_741_824L));
    Assert.assertEquals(62, BitUtil.ntz3(4_611_686_018_427_387_904L));
    Assert.assertEquals(0, BitUtil.ntz3(1L));
  }

  @Test
  public void testNtz() {
    Assert.assertEquals(47, BitUtil.ntz(-3_992_300_332_175_589_376L));
    Assert.assertEquals(30, BitUtil.ntz(-3_992_440_518_834_388_992L));
    Assert.assertEquals(16, BitUtil.ntz(-3_992_440_518_834_323_456L));
    Assert.assertEquals(51, BitUtil.ntz(-3_992_441_069_663_944_704L));
    Assert.assertEquals(39, BitUtil.ntz(-3_992_440_519_908_130_816L));
    Assert.assertEquals(59, BitUtil.ntz(-4_035_225_266_123_964_416L));
    Assert.assertEquals(11, BitUtil.ntz(51_200));
    Assert.assertEquals(29, BitUtil.ntz(536_870_912));
    Assert.assertEquals(23, BitUtil.ntz(545_259_520));
    Assert.assertEquals(15, BitUtil.ntz(4_295_000_064L));
    Assert.assertEquals(0, BitUtil.ntz(1));
    Assert.assertEquals(0, BitUtil.ntz(1L));
  }

  @Test
  public void testPop_andnot() {
    final long[] A = {};
    final long[] B = {};

    Assert.assertEquals(0L, BitUtil.pop_andnot(A, B, 0, 0));
  }

  @Test
  public void testPop_andnot2() {
    final long[] A = {
        9_136_202_764_425_034_697L, 9_136_202_764_425_034_699L, 9_136_202_764_425_034_699L,
        9_136_202_764_425_034_697L, 9_136_202_764_425_034_699L, 9_136_202_764_425_034_699L,
        9_136_202_764_425_034_697L, 9_136_202_764_425_034_699L, 9_136_202_764_425_034_699L};
    final long[] B = {9_208_264_345_274_743_755L, 9_208_264_345_274_743_753L,
                      9_208_264_345_274_743_759L, 9_208_264_345_274_743_753L,
                      9_208_264_345_274_743_759L, 9_208_264_344_201_001_931L,
                      9_208_264_345_274_743_753L, 9_208_264_345_274_743_759L,
                      9_208_264_345_274_743_755L, 9_208_264_345_274_743_755L};

    Assert.assertEquals(0L, BitUtil.pop_andnot(A, B, 3, 6));
  }

  @Test
  public void testPop_andnot3() {
    final long[] A = {9_145_121_593_754_583_004L,  9_136_110_065_172_322_814L,
                      -86_694_311_215_260_193L,    -86_694_310_149_357_572L,
                      -1_239_897_289_733_455_874L, -86_694_310_141_558_818L,
                      9_136_677_726_704_803_837L,  -86_694_310_141_501_953L,
                      -86_694_311_081_091_105L,    -86_694_310_141_501_985L};
    final long[] B = {8_326_311_286_078_603_626L,  8_614_541_799_710_556_394L,
                      8_335_037_066_191_103_832L,  -888_013_951_923_093_398L,
                      8_335_318_502_520_946_792L,  8_614_260_187_294_892_266L,
                      -1_032_164_148_000_366_214L, 8_614_260_187_295_154_410L,
                      8_335_318_485_333_606_506L,  8_614_541_662_271_602_922L};

    Assert.assertEquals(39L, BitUtil.pop_andnot(A, B, 9, 1));
  }

  @Test
  public void testPop_andnot4() {
    final long[] A = {9_145_121_609_994_928_089L,  9_136_110_081_413_217_273L,
                      -86_694_293_900_623_911L,    -86_694_293_909_012_487L,
                      -1_239_897_273_492_578_823L, -86_694_293_900_714_535L,
                      9_136_677_742_945_697_275L,  -87_257_243_854_110_725L,
                      -86_694_293_900_705_831L,    9_136_677_742_954_127_321L};
    final long[] B = {8_902_772_039_455_611_363L, -311_874_135_682_155_839L,
                      -311_874_273_121_109_311L,  8_911_779_238_710_352_099L,
                      8_911_779_238_718_216_417L, 8_911_779_238_718_216_417L,
                      8_767_664_050_642_901_219L, 8_911_779_238_718_478_561L,
                      8_911_779_238_710_614_243L, 8_911_497_763_741_505_761L};

    Assert.assertEquals(315L, BitUtil.pop_andnot(A, B, 2, 8));
  }

  @Test
  public void testPop_array() {
    final long[] A = {0L};

    Assert.assertEquals(0L, BitUtil.pop_array(A, 0, 0));
  }

  @Test
  public void testPop_array2() {
    final long[] A = {-2_305_923_485_115_177_757L, -7_017_458_282_802_022_451L,
                      3_917_207_308_350_450_274L,  8_349_015_784_081_012_100L,
                      6_772_628_242_015_071_271L,  385_491_374_688_347_319L,
                      6_338_880_024_203_469_254L,  6_294_134_920_194_430_421L,
                      8_935_000_235_501_318_373L,  -7_017_458_282_802_022_459L};

    Assert.assertEquals(83L, BitUtil.pop_array(A, 8, 2));
  }

  @Test
  public void testPop_array3() {
    final long[] A = {3_314_649_334_871_491_072L,  2_305_843_834_384_286_208L,
                      6_917_529_027_641_090_817L,  3_170_538_613_159_059_968L,
                      -7_926_334_519_538_261_504L, 90_624L,
                      4_612_248_968_382_996_992L,  8_361_496_225_639_326_465L,
                      8_361_496_225_639_326_465L,  8_073_265_557_431_910_913L};

    Assert.assertEquals(31L, BitUtil.pop_array(A, 3, 4));
  }

  @Test
  public void testPop_array4() {
    final long[] A = {8_646_582_454_478_373_033L,  8_646_582_454_478_373_033L,
                      -576_789_724_243_230_167L,   -36_375_640_318_078_816L,
                      -288_526_495_845_086_037L,   -2_351_791_912_583_717_974L,
                      -613_522_509_352_860_512L,   -117_633_737_187_045_372L,
                      -5_810_555_876_640_241_202L, -288_632_323_872_813_909L};

    Assert.assertEquals(296L, BitUtil.pop_array(A, 1, 9));
  }

  @Test
  public void testPop_intersect() {
    final long[] A = {};
    final long[] B = {0L};

    Assert.assertEquals(0L, BitUtil.pop_intersect(A, B, 0, 0));
  }

  @Test
  public void testPop_intersect2() {
    final long[] A = {5_019_606_882_484_051_799L,  5_164_289_400_976_277_077L,
                      -2_451_084_373_271_364_364L, -2_451_084_373_271_364_364L,
                      -2_451_084_373_271_364_364L, -2_306_969_185_195_508_492L,
                      -2_451_084_373_271_364_364L, -2_451_084_373_271_364_364L,
                      -3_351_945_313_251_230_466L, -235_885_139_150_312_450L};
    final long[] B = {-46_243_260_590_464_017L,    -46_243_260_590_464_017L,
                      -334_473_636_742_175_761L,   -334_473_636_742_175_761L,
                      -1_824_030_445_522_047_141L, -334_473_636_742_175_761L,
                      -1_824_030_445_522_047_141L, -1_824_030_445_522_047_141L,
                      -1_310_785_046_906_474_649L, -1_977_283_259_600_507_565L};

    Assert.assertEquals(179L, BitUtil.pop_intersect(A, B, 1, 6));
  }

  @Test
  public void testPop_intersect3() {
    final long[] A = {-613_134_011_317_090_827L,   -617_567_242_200_283_723L,
                      -9_581_292_505_262_667L,     -63_624_488_033_756_683L,
                      -1_188_952_647_219_275_779L, -1_197_959_846_473_869_315L,
                      -577_094_208_444_217_411L,   -586_101_407_699_085_313L,
                      -4_050_987_868_540_375_113L, -378_674_171_288_288_797L};
    final long[] B = {6_838_574_700_617_509_719L,  -2_384_796_786_481_448_105L,
                      -2_416_326_381_834_994_859L, -3_262_875_568_432_873_707L,
                      -2_310_501_136_844_198_637L, -2_305_856_798_671_668_139L,
                      -2_342_511_218_879_501_281L, -2_309_473_195_207_967_715L,
                      -37_335_603_098_943_907L,    -37_335_603_098_943_907L};

    Assert.assertEquals(265L, BitUtil.pop_intersect(A, B, 1, 9));
  }

  @Test
  public void testPop_intersect4() {
    final long[] A = {-2_604_155_907_533_352_475L, -9_436_009_347_867_163L,
                      -11_540_508_946_244_115L,    -9_436_043_690_682_911L,
                      -298_088_632_307_591_699L,   -9_290_908_151_621_651L,
                      -298_018_263_026_280_987L,   -298_154_568_104_192_539L,
                      9_213_938_052_449_801_441L,  -2_315_851_077_206_079_512L};
    final long[] B = {9_175_835_018_600_623_959L,  6_869_407_069_268_781_911L,
                      -2_353_254_648_580_609_185L, -2_351_062_256_754_512_041L,
                      -2_351_484_435_263_389_857L, -2_351_688_945_370_009_769L,
                      -12_073_336_187_716_009L,    -2_353_384_425_313_010_089L,
                      -1_224_122_267_910_824_609L, -1_457_643_144_751_105_449L};

    Assert.assertEquals(272L, BitUtil.pop_intersect(A, B, 0, 10));
  }

  @Test
  public void testPop_union() {
    final long[] A = {};
    final long[] B = {0L};

    Assert.assertEquals(0L, BitUtil.pop_union(A, B, 0, 0));
  }

  @Test
  public void testPop_union2() {
    final long[] A = {-8_414_946_420_341_651_882L, -8_998_109_258_814_762_138L,
                      823_177_991_133_794_308L,    1_060_119_449_135_812_357L,
                      4_936_156_383_985_274_116L,  6_542_628_643_604_005_380L,
                      5_813_161_990_911_297_093L,  14_637_861_903_008_828L,
                      1_893_915_386_111_861_252L,  4_936_720_192_932_161_284L};
    final long[] B = {
        -8_646_202_919_596_293_214L, 2_885_276_853_847_853_059L,  1_189_891_526_562_431_433L,
        1_155_454_779_430_846_536L,  -6_322_055_513_130_975_037L, 18_331_337_837_083_737L,
        -7_436_250_564_393_353_200L, -6_917_247_238_703_660_538L, 1_155_454_779_430_846_536L};

    Assert.assertEquals(148L, BitUtil.pop_union(A, B, 1, 4));
  }

  @Test
  public void testPop_union3() {
    final long[] A = {4_294_766_553_297_212_427L,  4_276_743_046_915_314_699L,
                      164_673_873_965_879_313L,    2_452_834_539_186_295_056L,
                      2_388_186_847_097_640_408L,  3_976_967_951_906_608_600L,
                      6_794_108_043_655_828_498L,  6_873_483_694_702_055_498L,
                      -1_665_606_725_510_262_181L, -2_836_279_284_677_365_742L};
    final long[] B = {
        5_188_147_045_628_117_248L, 4_665_729_489_373_266_944L, 5_265_553_512_245_117_986L,
        5_319_631_049_257_927_715L, 5_242_190_241_959_773_219L, 4_958_885_678_806_859_815L,
        6_755_644_807_970_884L,     18_436_822_292_497_413L,    6_755_644_807_970_884L};

    Assert.assertEquals(324L, BitUtil.pop_union(A, B, 0, 9));
  }

  @Test
  public void testPop_union4() {
    final long[] A = {17_612_115_649_006_752L,    28_317_100_577_672_548L,
                      2_597_031_072_046_809_232L, 2_597_031_072_046_809_232L,
                      2_598_156_971_685_219_552L, 2_598_156_971_685_219_552L,
                      2_597_031_072_046_809_232L, 2_598_156_971_685_219_552L,
                      2_624_053_083_067_392_740L, 2_597_031_072_046_809_232L};
    final long[] B = {281_474_976_710_660L,     36_310_271_995_674_628L, 109_493_766_141_824_038L,
                      109_493_766_141_824_038L, 36_310_271_995_674_625L, 36_310_271_995_674_625L,
                      578_431_372_507_499_536L, 36_310_271_995_674_625L, 39_406_496_925_549_056L,
                      36_310_271_995_674_625L};

    Assert.assertEquals(200L, BitUtil.pop_union(A, B, 0, 10));
  }

  @Test
  public void testPop_xor() {
    final long[] A = {};
    final long[] B = {};

    Assert.assertEquals(0L, BitUtil.pop_xor(A, B, 0, 0));
  }

  @Test
  public void testPop_xor2() {
    final long[] A = {-8_645_741_129_299_181_567L, -9_222_209_577_647_128_576L,
                      -9_223_372_036_718_247_935L, -9_222_246_128_221_470_719L,
                      -9_222_089_696_922_632_192L, -8_645_740_029_250_682_880L};
    final long[] B = {-8_069_324_632_204_558_336L, -9_222_209_578_183_999_488L,
                      -9_223_372_036_718_247_935L, -9_222_246_136_811_405_311L,
                      -9_222_245_861_933_514_752L, -8_069_324_632_204_558_336L,
                      -8_069_324_632_204_558_335L};

    Assert.assertEquals(15L, BitUtil.pop_xor(A, B, 1, 5));
  }

  @Test
  public void testPop_xor3() {
    final long[] A = {16_385L, 16_384L, 16_384L, 16_385L, 16_386L, 16_384L};
    final long[] B = {16_384L, 16_384L, 16_385L, 0L, 16_385L, 16_384L, 16_385L, 16_385L};

    Assert.assertEquals(0L, BitUtil.pop_xor(A, B, 5, 1));
  }

  @Test
  public void testPop_xor4() {
    final long[] A = {-9_187_164_834_945_089_535L, -8_321_375_302_963_740_672L,
                      -8_601_840_103_752_253_440L, -8_321_480_856_080_007_168L,
                      -8_601_734_550_635_986_944L, -8_321_337_919_568_396_288L,
                      -8_600_747_189_194_260_480L, -8_321_476_458_033_496_064L,
                      -8_601_875_288_124_358_656L, -8_303_323_521_058_914_304L};
    final long[] B = {-7_744_913_451_551_342_592L, -8_321_478_657_056_751_616L,
                      -8_601_732_351_092_637_696L, -8_321_478_657_056_751_616L,
                      -8_601_732_351_092_637_696L, -7_168_416_414_827_331_584L,
                      -7_447_680_549_069_324_288L, -8_321_474_258_876_022_784L,
                      -8_601_875_288_141_135_872L, -7_746_039_351_458_185_216L};

    Assert.assertEquals(13L, BitUtil.pop_xor(A, B, 3, 6));
  }

  @Test
  public void testPop_xor5() {
    final long[] A = {-9_187_164_834_945_089_535L, -8_321_375_302_963_740_672L,
                      -8_600_573_465_818_611_711L, -8_897_937_210_336_919_296L,
                      -8_601_694_968_211_077_824L, -8_321_340_119_127_293_952L,
                      -8_601_837_903_655_272_432L, -8_322_497_078_481_156_863L,
                      -8_600_712_003_765_190_655L, -8_303_323_521_058_914_304L};
    final long[] B = {-7_746_001_965_378_551_804L, -8_321_375_576_231_034_860L,
                      -8_600_714_167_302_503_084L, -8_321_476_456_422_849_451L,
                      -8_601_694_965_281_374_204L, -8_321_341_215_955_861_420L,
                      -8_601_837_900_987_695_036L, -8_898_957_554_308_988_848L,
                      -8_600_707_568_674_144_256L, -7_744_876_065_471_709_180L};

    Assert.assertEquals(81L, BitUtil.pop_xor(A, B, 1, 8));
  }

  @Test
  public void testPop() {
    Assert.assertEquals(0, BitUtil.pop(0L));
    Assert.assertEquals(2, BitUtil.pop(3L));
  }
}
