package com.graphiq.pdi.coalesce;

import org.junit.Assert;
import org.junit.Test;
import org.pentaho.di.TestFailedException;
import org.pentaho.di.TestUtilities;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.*;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CoalesceStepTest {

	private String[] fieldNames;
	private String[] fieldTypes;
	ValueMetaInterface[] valuesMeta;
	private List<List<Object>> inputRows;

	@Test
	public void testCoalesce() throws Exception {
		processInputTestFile( "phone_numbers.txt" );

		List<RowMetaAndData> transformationResults = test( true, ValueMeta.TYPE_STRING );
		List<RowMetaAndData> expectedResults = createExpectedResults( 1 );

		try {
			TestUtilities.checkRows( transformationResults, expectedResults, 0 );
		} catch ( TestFailedException tfe ) {
			Assert.fail( tfe.getMessage() );
		}
	}

	@Test
	public void testDefaultValueMeta() throws Exception {
		processInputTestFile( "phone_numbers.txt" );

		List<RowMetaAndData> transformationResults = test( true, ValueMeta.TYPE_NONE );
		List<RowMetaAndData> expectedResults = createExpectedResults( 1 );

		try {
			TestUtilities.checkRows( transformationResults, expectedResults, 0 );
		} catch ( TestFailedException tfe ) {
			Assert.fail( tfe.getMessage() );
		}
	}

	@Test
	public void testNumberFromString() throws Exception {
		processInputTestFile( "average_temperatures.txt" );

		List<RowMetaAndData> transformationResults = test( false, ValueMeta.TYPE_NUMBER );
		List<RowMetaAndData> expectedResults = createExpectedResults( 2 );

		try {
			TestUtilities.checkRows( transformationResults, expectedResults, 0 );
		} catch ( TestFailedException tfe ) {
			Assert.fail( tfe.getMessage() );
		}
	}

	private List<RowMetaAndData> test( boolean remove, int valueType ) throws KettleException {

		KettleEnvironment.init();

		// Create a new transformation
		TransMeta transMeta = new TransMeta();
		transMeta.setName( "testCoalesce" );
		PluginRegistry registry = PluginRegistry.getInstance();

		// Create Injector
		String injectorStepName = "injector step";
		StepMeta injectorStep = TestUtilities.createInjectorStep( injectorStepName, registry );
		transMeta.addStep( injectorStep );

		// Create a Coalesce step
		String coalesceStepName = "coalesce step";
		StepMeta coalesceStep = createCoalesceMeta( coalesceStepName, registry, remove, valueType );
		transMeta.addStep( coalesceStep );

		// TransHopMeta between injector step and CoalesceStep
		TransHopMeta injectorToCoalesceHop = new TransHopMeta( injectorStep, coalesceStep );
		transMeta.addTransHop( injectorToCoalesceHop );

		// Create a dummy step
		String dummyStepName = "dummy step";
		StepMeta dummyStep = TestUtilities.createDummyStep( dummyStepName, registry );
		transMeta.addStep( dummyStep );

		// TransHopMeta between CoalesceStep and DummyStep
		TransHopMeta coalesceToDummyHop = new TransHopMeta( coalesceStep, dummyStep );
		transMeta.addTransHop( coalesceToDummyHop );

		// Execute the transformation
		Trans trans = new Trans( transMeta );
		trans.prepareExecution( null );

		// Create a row collector and add it to the dummy step interface
		StepInterface si = trans.getStepInterface( dummyStepName, 0 );
		RowStepCollector dummyRowCollector = new RowStepCollector();
		si.addRowListener( dummyRowCollector );

		// Create a row producer
		RowProducer rowProducer = trans.addRowProducer( injectorStepName, 0 );
		trans.startThreads();

		// create the rows
		List<RowMetaAndData> inputList = createInputData();
		for ( RowMetaAndData rowMetaAndData : inputList ) {
			rowProducer.putRow( rowMetaAndData.getRowMeta(), rowMetaAndData.getData() );
		}
		rowProducer.finished();

		trans.waitUntilFinished();

		return dummyRowCollector.getRowsWritten();
	}


	private StepMeta createCoalesceMeta( String name, PluginRegistry registry, boolean removeInputFields, int valueType ) {

		CoalesceMeta coalesceMeta = new CoalesceMeta();

		coalesceMeta.setOutputFields( new String[] { "out" } );
		coalesceMeta.setValueType( new int[] { valueType } );
		coalesceMeta.setDoRemoveInputFields( new boolean[] { removeInputFields } );
		//input fields
		String[][] inputFields = new String[1][CoalesceMeta.noInputFields];
		for ( int i = 0; i < CoalesceMeta.noInputFields; i++ ) {
			inputFields[0][i] = fieldNames[i];
		}
		coalesceMeta.setInputFields( inputFields );

		String pluginId = registry.getPluginId( StepPluginType.class, coalesceMeta );

		return new StepMeta( pluginId, name, coalesceMeta );
	}

	private List<RowMetaAndData> createInputData() {
		List<RowMetaAndData> list = new ArrayList<RowMetaAndData>();
		RowMetaInterface rowMeta = createRowMetaInterface( valuesMeta );

		for ( List r : inputRows ) {
			list.add( new RowMetaAndData( rowMeta, r.toArray() ) );
		}
		return list;
	}

	private RowMetaInterface createRowMetaInterface( ValueMetaInterface[] valuesMeta ) {
		RowMetaInterface rowMeta = new RowMeta();
		for ( ValueMetaInterface aValuesMeta : valuesMeta ) {
			rowMeta.addValueMeta( aValuesMeta );
		}

		return rowMeta;
	}


	private void processInputTestFile( String file ) throws Exception {
		BufferedReader reader = new BufferedReader(
			new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream( file ) ) );

		fieldNames = reader.readLine().split( "," );
		fieldTypes = reader.readLine().split( "," );
		valuesMeta = new ValueMetaInterface[CoalesceMeta.noInputFields];
		for ( int i = 0; i < fieldNames.length; i++ ) {
			valuesMeta[i] = new ValueMeta( fieldNames[i], Integer.parseInt( fieldTypes[i] ) );
		}

		inputRows = new ArrayList<List<Object>>();
		String line;
		ValueMetaInterface stringValueMeta = new ValueMeta( "forConversionOnly", ValueMeta.TYPE_STRING );
		while ( ( line = reader.readLine() ) != null ) {
			String[] stringValues = parseValuesFromStringRow( line );
			List objectValues = new ArrayList<Object>();
			for ( int i = 0; i < stringValues.length; i++ ) {
				objectValues.add( valuesMeta[i].convertData( stringValueMeta, stringValues[i] ) );
			}
			inputRows.add( objectValues );
		}

		reader.close();
	}

	/**
	 * Creates result data.
	 *
	 * @return list of metadata/data couples of how the result should look.
	 */
	private List<RowMetaAndData> createExpectedResults( int testCase ) {
		List<RowMetaAndData> list = new ArrayList<RowMetaAndData>();
		List<ValueMetaInterface> valuesMeta = new ArrayList<ValueMetaInterface>();
		Object[][] resultRows = new Object[inputRows.size()][];

		switch ( testCase ) {
			case 1:
				valuesMeta.add( new ValueMeta( "out", ValueMeta.TYPE_STRING ) );
				resultRows[0] = new Object[] { "248-0532" };
				resultRows[1] = new Object[] { "125-2044" };
				resultRows[2] = new Object[] { "216-9620" };
				resultRows[3] = new Object[] { null };
				break;

			case 2:
				valuesMeta.addAll( Arrays.asList( this.valuesMeta ) );
				valuesMeta.add( new ValueMeta( "temperature", ValueMeta.TYPE_NUMBER ) );
				resultRows[0] = new Object[] { 10.5d, "6", 8d };
				resultRows[1] = new Object[] { null, "7.5", 9d };
				resultRows[2] = new Object[] { null, null, 10.5d };
				resultRows[3] = new Object[] { null, null, null };
				break;
		}

		RowMetaInterface rowMeta = createRowMetaInterface( valuesMeta.toArray( new ValueMetaInterface[valuesMeta.size()] ) );
		for ( Object[] r : resultRows ) {
			list.add( new RowMetaAndData( rowMeta, r ) );
		}

		return list;
	}

	private String[] parseValuesFromStringRow( String line ) {
		String[] values = line.split( "," );

		for ( int i = 0; i < values.length; i++ ) {
			if ( values[i].equals( "\"\"" ) ) {
				values[i] = "";
			} else if ( values[i].isEmpty() ) {
				values[i] = null;
			}
		}

		//for a ",," input String.split returns 0 size array
		if ( values.length == 0 ) {
			return new String[line.length() + 1];
		}

		return values;
	}
}
