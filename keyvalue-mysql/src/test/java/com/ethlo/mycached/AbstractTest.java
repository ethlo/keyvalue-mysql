package com.ethlo.mycached;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

import com.ethlo.keyvalue.DataCompressor;
import com.ethlo.keyvalue.HexKeyEncoder;
import com.ethlo.keyvalue.KeyEncoder;
import com.ethlo.keyvalue.NopDataCompressor;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = { "classpath:/mycached-testcontext.xml" })
@TestExecutionListeners(listeners ={ DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class, TransactionalTestExecutionListener.class })
@Transactional
public abstract class AbstractTest
{
    protected KeyEncoder keyEncoder = new HexKeyEncoder();
    protected DataCompressor dataCompressor = new NopDataCompressor();
}
