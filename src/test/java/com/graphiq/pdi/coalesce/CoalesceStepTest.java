package com.graphiq.pdi.coalesce;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.StepMockUtil;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

public class CoalesceStepTest {

    private CoalesceStep step;
    private final Object[] inputRow = new Object[] { "a string" };

    @BeforeClass
    public static void initKettle() throws Exception {
        KettleEnvironment.init();
    }

    @Before
    public void setUp() throws Exception {
        StepMockHelper<CoalesceMeta, StepDataInterface> helper =
                StepMockUtil.getStepMockHelper(CoalesceMeta.class, "CoalesceStepTest");

        when( helper.stepMeta.isDoingErrorHandling() ).thenReturn( true );

        step = new CoalesceStep(helper.stepMeta, helper.stepDataInterface, 1, helper.transMeta, helper.trans);
        step = spy( step );

        doReturn(inputRow).when(step).getRow();
        doNothing().when(step)
            .putError(any(RowMetaInterface.class), any(Object[].class), anyLong(), anyString(), anyString(), anyString());
    }

    @Test
    public void dummyTest() {
        assertEquals(true, true);
    }
}
