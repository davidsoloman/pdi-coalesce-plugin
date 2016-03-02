/******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package com.graphiq.pdi.coalesce;

import java.util.Arrays;
import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step(
		id = "CoalesceStep",
		image = "coalesce.svg",
		i18nPackageName="com.graphiq.pdi.coalesce",
		name="Coalesce.Name",
		description = "Coalesce.TooltipDesc",
		categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Transform"
)
public class CoalesceMeta extends BaseStepMeta implements StepMetaInterface {

	/**
	 * The PKG member is used when looking up internationalized strings.
	 * The properties file with localized keys is expected to reside in
	 * {the package of the class specified}/com.graphiq.pdi.coalesce.messages/messages_{locale}.properties
	 */
	private static Class<?> PKG = CoalesceMeta.class; // for i18n purposes


	/**
	 * constants:
	 */
	private static final int STRING_AS_DEFAULT = -1;
	static final int noInputFields = 3;

	/**
	 * Stores the name of the field added to the row-stream.
	 */
	private String[] outputFields;
	private String[][] inputFields;
	private int[] valueType;
	private boolean[] doRemoveInputFields;

	/**
	 * additional options
	 */
	private boolean treatEmptyStringsAsNulls;

	public CoalesceMeta() {
		super();
	}

	/**
	 * Called by PDI to get a new instance of the step implementation.
	 * A standard implementation passing the arguments to the constructor of the step class is recommended.
	 *
	 * @param stepMeta          description of the step
	 * @param stepDataInterface instance of a step data class
	 * @param cnr               copy number
	 * @param transMeta         description of the transformation
	 * @param disp              runtime implementation of the transformation
	 * @return the new instance of a step implementation
	 */
	@Override
	public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp ) {
		return new CoalesceStep( stepMeta, stepDataInterface, cnr, transMeta, disp );
	}

	/**
	 * Called by PDI to get a new instance of the step data class.
	 */
	@Override
	public StepDataInterface getStepData() {
		return new CoalesceData();
	}

	/**
	 * This method is called every time a new step is created and should allocate/set the step configuration
	 * to sensible defaults. The values set here will be used by Spoon when a new step is created.
	 */
	@Override
	public void setDefault() {
		allocate( 0 );
	}

	public String[] getOutputFields() {
		return outputFields;
	}
	public void setOutputFields( String[] outputFields ) {
		this.outputFields = outputFields;
	}

	public String[][] getInputFields() {
		return inputFields;
	}
	public void setInputFields( String[][] inputFields ) {
		this.inputFields = inputFields;
	}

	public int[] getValueType() {
		return valueType;
	}
	public void setValueType( int[] valueType ) {
		this.valueType = valueType;
	}

	public boolean[] getDoRemoveInputFields() {
		return doRemoveInputFields;
	}
	public void setDoRemoveInputFields( boolean[] doRemoveInputFields ) {
		this.doRemoveInputFields = doRemoveInputFields;
	}

	public boolean isTreatEmptyStringsAsNulls() {
		return treatEmptyStringsAsNulls;
	}
	public void setTreatEmptyStringsAsNulls( boolean treatEmptyStringsAsNulls ) {
		this.treatEmptyStringsAsNulls = treatEmptyStringsAsNulls;
	}

	/**
	 * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
	 * step meta object.
	 *
	 * @return a deep copy of this
	 */
	@Override
	public Object clone() {
		CoalesceMeta retVal = (CoalesceMeta) super.clone();

		int nrFields = outputFields.length;
		retVal.outputFields = Arrays.copyOf( outputFields, nrFields );

		retVal.inputFields = new String[noInputFields][];
		for ( int i = 0; i < nrFields; i++ ) {
			retVal.inputFields[i] = Arrays.copyOf( inputFields[i], inputFields[i].length );
		}

		retVal.valueType = Arrays.copyOf( valueType, nrFields );
		retVal.doRemoveInputFields = Arrays.copyOf( doRemoveInputFields, nrFields );

		return retVal;
	}

	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
	 * return value is an XML fragment consisting of one or more XML tags.
	 *
	 * @return a string containing the XML serialization of this step
	 */
	@Override
	public String getXML() throws KettleValueException {

		StringBuilder retVal = new StringBuilder( 500 );

		retVal.append( "    " + XMLHandler.addTagValue( "empty_is_null", treatEmptyStringsAsNulls ) );

		retVal.append( "    <fields>" ).append( Const.CR );
		for ( int i = 0; i < outputFields.length; i++ ) {
			retVal.append( "      <field>" ).append( Const.CR );
			retVal.append( "        " ).append( XMLHandler.addTagValue( "output_field", outputFields[i] ) );
			retVal.append( "        " ).append( XMLHandler.addTagValue( "value_type", ValueMeta.getTypeDesc( valueType[i] ) ) );
			retVal.append( "        " ).append( XMLHandler.addTagValue( "remove", getStringFromBoolean( doRemoveInputFields[i] ) ) );
			for ( int j = 0; j < noInputFields; j++ ) {
				retVal.append( "        " ).append( XMLHandler.addTagValue( getInputFieldTag( j ), inputFields[i][j] ) );
			}
			retVal.append( "      </field>" ).append( Const.CR );
		}
		retVal.append( "    </fields>" ).append( Const.CR );

		return retVal.toString();
	}

	/**
	 * This method is called by PDI when a step needs to load its configuration from XML.
	 *
	 * @param stepNode  the XML node containing the configuration
	 * @param databases the databases available in the transformation
	 * @param metaStore the metaStore to optionally read from
	 */
	@Override
	public void loadXML( Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

		try {
			treatEmptyStringsAsNulls = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, "empty_is_null" ) );

			Node fields = XMLHandler.getSubNode( stepNode, "fields" );
			int noFields = XMLHandler.countNodes( fields, "field" );
			allocate( noFields );

			for ( int i = 0; i < noFields; i++ ) {
				Node line = XMLHandler.getSubNodeByNr( fields, "field", i );
				outputFields[i] = Const.NVL( XMLHandler.getTagValue( line, "output_field" ), "" );
				valueType[i] = ValueMeta.getType( XMLHandler.getTagValue( line, "value_type" ) );
				doRemoveInputFields[i] = getBooleanFromString( XMLHandler.getTagValue( line, "remove" ) );
				for ( int j = 0; j < noInputFields; j++ ) {
					inputFields[i][j] = Const.NVL( XMLHandler.getTagValue( line, getInputFieldTag( j ) ), "" );
				}
			}
		} catch ( Exception e ) {
			throw new KettleXMLException( BaseMessages.getString(
							PKG, "CoalesceMeta.Exception.UnableToReadStepInfoFromXML" ), e );
		}

	}

	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to a repository.
	 * The repository implementation provides the necessary methods to save the step attributes.
	 *
	 * @param rep               the repository to save to
	 * @param metaStore         the metaStore to optionally write to
	 * @param id_transformation the id to use for the transformation when saving
	 * @param id_step           the id to use for the step  when saving
	 */
	@Override
	public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
		try {
			rep.saveStepAttribute( id_transformation, id_step, "empty_is_null", treatEmptyStringsAsNulls );

			for ( int i = 0; i < outputFields.length; i++ ) {
				rep.saveStepAttribute( id_transformation, id_step, i, "output_field", outputFields[i] );
				rep.saveStepAttribute( id_transformation, id_step, i, "value_type", ValueMeta.getTypeDesc( valueType[i] ) );
				rep.saveStepAttribute( id_transformation, id_step, i, "remove", getStringFromBoolean( doRemoveInputFields[i] ) );
				for ( int j = 0; j < noInputFields; j++ ) {
					rep.saveStepAttribute( id_transformation, id_step, i, getInputFieldTag( j ), inputFields[i][j] );
				}
			}
		} catch ( Exception e ) {
			throw new KettleException( "Unable to save step into repository: " + id_step, e );
		}
	}

	/**
	 * This method is called by PDI when a step needs to read its configuration from a repository.
	 * The repository implementation provides the necessary methods to read the step attributes.
	 *
	 * @param rep       the repository to read from
	 * @param metaStore the metaStore to optionally read from
	 * @param id_step   the id of the step being read
	 * @param databases the databases available in the transformation
	 */
	@Override
	public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
		try {
			treatEmptyStringsAsNulls = rep.getStepAttributeBoolean( id_step, getRepCode( "empty_is_null" ) );

			int nrFields = rep.countNrStepAttributes( id_step, getRepCode( "output_field" ) );
			allocate( nrFields );

			for ( int i = 0; i < nrFields; i++ ) {
				outputFields[i] = rep.getStepAttributeString( id_step, i, getRepCode( "output_field" ) );
				valueType[i] = ValueMeta.getType( rep.getStepAttributeString( id_step, i, getRepCode( "value_type" ) ) );
				doRemoveInputFields[i] = getBooleanFromString( rep.getStepAttributeString( id_step, i, getRepCode( "remove" ) ) );
				for ( int j = 0; j < noInputFields; j++ ) {
					inputFields[i][j] = rep.getStepAttributeString( id_step, i, getRepCode( getInputFieldTag( j ) ) );
				}
			}
		} catch ( Exception e ) {
			throw new KettleException( "Unable to load step from repository", e );
		}
	}

	/**
	 * This method is called to determine the changes the step is making to the row-stream.
	 *
	 * @param inputRowMeta the row structure coming in to the step
	 * @param name         the name of the step making the changes
	 * @param info         row structures of any info steps coming in
	 * @param nextStep     the description of a step this step is passing rows to
	 * @param space        the variable space for resolving variables
	 * @param repository   the repository instance optionally read from
	 * @param metaStore    the metaStore to optionally read from
	 */
	@Override
	public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
					VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
		try {
			// store the input stream meta
			RowMetaInterface unalteredInputRowMeta = inputRowMeta.clone();

			// first remove all unwanted input fields from the stream
			for ( int i = 0; i < outputFields.length; i++ ) {
				if ( doRemoveInputFields[i] ) {
					for ( int j = 0; j < noInputFields; j++ ) {
						if ( inputRowMeta.indexOfValue( inputFields[i][j] ) != -1 ) {
							inputRowMeta.removeValueMeta( inputFields[i][j] );
						}
					}
				}
			}

			// then add the output fields
			for ( int i = 0; i < outputFields.length; i++ ) {
				int type = valueType[i];
				if ( type == ValueMeta.TYPE_NONE ) {
					type = getDefaultValueType( unalteredInputRowMeta, i );
				}

				ValueMetaInterface v = ValueMetaFactory.createValueMeta( outputFields[i], type );
				v.setOrigin( name );
				inputRowMeta.addValueMeta( v );
			}
		} catch ( Exception e ) {
			throw new KettleStepException( e );
		}
	}

	/**
	 * This method is called when the user selects the "Verify Transformation" option in Spoon.
	 *
	 * @param remarks   the list of remarks to append to
	 * @param transMeta the description of the transformation
	 * @param stepMeta  the description of the step
	 * @param prev      the structure of the incoming row-stream
	 * @param input     names of steps sending input to the step
	 * @param output    names of steps this step is sending output to
	 * @param info      fields coming in from info steps
	 * @param metaStore metaStore to optionally read from
	 */
	@Override
	public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
					RowMetaInterface prev, String input[], String output[], RowMetaInterface info,
					VariableSpace space, Repository repository, IMetaStore metaStore ) {

		CheckResult cr;

		// See if there are input streams leading to this step!
		if ( input.length > 0 ) {
			cr = new CheckResult( CheckResult.TYPE_RESULT_OK,
							BaseMessages.getString( PKG, "CoalesceMeta.CheckResult.ReceivingRows.OK" ), stepMeta );
		} else {
			cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR,
							BaseMessages.getString( PKG, "CoalesceMeta.CheckResult.ReceivingRows.ERROR" ), stepMeta );
		}
		remarks.add( cr );

		// See if there are missing input streams
		String errorMessage = "";
		for ( int i = 0; i < outputFields.length; i++ ) {

			String missingFields = "";
			for ( int j = 0; j < noInputFields; j++ ) {
				ValueMetaInterface vmi = prev.searchValueMeta( inputFields[i][j] );

				if ( inputFields[i][j].isEmpty() && vmi == null ) {
					missingFields += inputFields[i][j] + Const.CR;
				}
			}

			if ( !missingFields.isEmpty() ) {
				errorMessage = BaseMessages.getString( PKG, "CoalesceMeta.CheckResult.MissingInStreamFields" ) +
								Const.CR + "\t\t" + missingFields;
				break;
			}
		}
		if ( !errorMessage.isEmpty() ) {
			cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, errorMessage, stepMeta );
		} else {
			cr = new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
							PKG, "CoalesceMeta.CheckResult.FoundInStreamFields" ), stepMeta );
		}
		remarks.add( cr );
	}

	void allocate( int noOutputFields ) {
		outputFields = new String[noOutputFields];
		inputFields = new String[noOutputFields][noInputFields];
		valueType = new int[noOutputFields];
		doRemoveInputFields = new boolean[noOutputFields];
	}

	static String getStringFromBoolean( boolean b ) {
		return b ? BaseMessages.getString( PKG, "System.Combo.Yes" )
						: BaseMessages.getString( PKG, "System.Combo.No" );
	}

	static boolean getBooleanFromString( String s ) {
		return BaseMessages.getString( PKG, "System.Combo.Yes" ).equals( s );
	}

	/**
	 * If all 3 fields are of the same data type then the output field should mirror this
	 * otherwise return a more generic String type
	 */
	private int getDefaultValueType( RowMetaInterface inputRowMeta, int rowIndex ) throws Exception {

		Integer valueType = null;
		int i = 0;
		do {
			if ( i == 0 ) {
				valueType = getInputFieldValueType( inputRowMeta, rowIndex, i++ );
			}
			Integer type = getInputFieldValueType( inputRowMeta, rowIndex, i );

			if ( ( valueType = getResultingType( valueType, type ) ) == STRING_AS_DEFAULT ) {
				return ValueMetaInterface.TYPE_STRING;
			}
		} while ( ++i < noInputFields );

		return valueType;
	}

	/**
	 * extracts the ValueMeta type of an input field,
	 * returns null if the field is not present in the input stream
	 */
	private Integer getInputFieldValueType( RowMetaInterface inputRowMeta, int rowIndex, int inputIndex ) {
		int index = inputRowMeta.indexOfValue( inputFields[rowIndex][inputIndex] );
		if ( index > 0 ) {
			return inputRowMeta.getValueMeta( index ).getType();
		}
		return null;
	}

	private Integer getResultingType( Integer typeA, Integer typeB ) {
		if ( typeA == null ) {
			return typeB;
		} else {
			if ( typeB == null ) {
				return typeA;
			}
			return typeA.equals( typeB ) ? typeA : STRING_AS_DEFAULT;
		}
	}

	private String getInputFieldTag( int index ) {
		return "input_field_" + (char) ( 'a' + index );
	}
}
