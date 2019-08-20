package com.nikey.test;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jayzeee
 * 测试一般的service和mapper都来继承这个类，例子参照MainElectricConnectionSetting包下的测试类
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:spring/applicationContext.xml" })
public abstract class AbstractJUnit {

}
