/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
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

import java.util.List;

import org.eclipse.swt.widgets.Shell;
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
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepMetaInterface.
 * Classes implementing this interface need to:
 * 
 * - keep track of the step settings
 * - serialize step settings both to xml and a repository
 * - provide new instances of objects implementing StepDialogInterface, StepInterface and StepDataInterface
 * - report on how the step modifies the meta-data of the row-stream (row structure and field types)
 * - perform a sanity-check on the settings provided by the user 
 * 
 */

@Step(	
		id = "CoalesceStep",
		image = "coalesce.png",
		i18nPackageName="com.graphiq.pdi.coalesce",
		name="Coalesce.Name",
		description = "Coalesce.TooltipDesc",
		categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Transform"
)
public class CoalesceMeta extends BaseStepMeta implements StepMetaInterface {

	/**
	 *	The PKG member is used when looking up internationalized strings.
	 *	The properties file with localized keys is expected to reside in 
	 *	{the package of the class specified}/com.graphiq.pdi.coalesce.messages/messages_{locale}.properties
	 */
	private static Class<?> PKG = CoalesceMeta.class; // for i18n purposes


	/**
	 * constants:
	 */
	private static final int STRING_AS_DEFAULT = -1;

	/**
	 * Stores the name of the field added to the row-stream. 
	 */
	private String[] outputFields;
	private String[] fieldsA;
	private String[] fieldsB;
	private String[] fieldsC;

	/**
	 * Constructor should call super() to make sure the base class has a chance to initialize properly.
	 */
	public CoalesceMeta() {
		super(); 
	}
	
	/**
	 * Called by Spoon to get a new instance of the SWT dialog for the step.
	 * A standard implementation passing the arguments to the constructor of the step dialog is recommended.
	 * 
	 * @param shell		an SWT Shell
	 * @param meta 		description of the step 
	 * @param transMeta	description of the the transformation 
	 * @param name		the name of the step
	 * @return 			new instance of a dialog for this step 
	 */
	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new CoalesceDialog(shell, meta, transMeta, name);
	}

	/**
	 * Called by PDI to get a new instance of the step implementation. 
	 * A standard implementation passing the arguments to the constructor of the step class is recommended.
	 * 
	 * @param stepMeta				description of the step
	 * @param stepDataInterface		instance of a step data class
	 * @param cnr					copy number
	 * @param transMeta				description of the transformation
	 * @param disp					runtime implementation of the transformation
	 * @return						the new instance of a step implementation 
	 */
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
		return new CoalesceStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	/**
	 * Called by PDI to get a new instance of the step data class.
	 */
	public StepDataInterface getStepData() {
		return new CoalesceData();
	}	

	/**
	 * This method is called every time a new step is created and should allocate/set the step configuration
	 * to sensible defaults. The values set here will be used by Spoon when a new step is created.
	 */
	public void setDefault() {
		allocate(0);
	}

	public String[] getOutputFields() {
		return outputFields;
	}

	public void setOutputFields(String[] outputFields) {
		this.outputFields = outputFields;
	}

	public String[] getFieldsA() {
		return fieldsA;
	}

	public void setFieldsA(String[] fieldsA) {
		this.fieldsA = fieldsA;
	}

	public String[] getFieldsB() {
		return fieldsB;
	}

	public void setFieldsB(String[] fieldsB) {
		this.fieldsB = fieldsB;
	}

	public String[] getFieldsC() {
		return fieldsC;
	}

	public void setFieldsC(String[] fieldsC) {
		this.fieldsC = fieldsC;
	}

	/**
	 * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
	 * step meta object. Be sure to create proper deep copies if the step configuration is stored in
	 * modifiable objects.
	 * 
	 * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
	 * a deep copy.
	 * 
	 * @return a deep copy of this
	 */
	public Object clone() {
		CoalesceMeta retval = (CoalesceMeta) super.clone();

		int nrfields = outputFields.length;

		retval.allocate(nrfields);

		for ( int i = 0; i < nrfields; i++ ) {
			retval.outputFields[i] = outputFields[i];
			retval.fieldsA[i] = fieldsA[i];
			retval.fieldsB[i] = fieldsB[i];
			retval.fieldsC[i] = fieldsC[i];
		}

		return retval;
	}
	
	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
	 * return value is an XML fragment consisting of one or more XML tags.  
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
	 * 
	 * @return a string containing the XML serialization of this step
	 */
	public String getXML() throws KettleValueException {

		StringBuffer retval = new StringBuffer( 500 );

		retval.append( "    <fields>" ).append(Const.CR);

		for ( int i = 0; i < outputFields.length; i++ ) {
			retval.append( "      <field>").append(Const.CR);
			retval.append( "        " ).append( XMLHandler.addTagValue( "output_field", outputFields[i]));
			retval.append( "        " ).append( XMLHandler.addTagValue( "field_a" , fieldsA[i] ) );
			retval.append( "        " ).append( XMLHandler.addTagValue( "field_b" , fieldsB[i] ) );
			retval.append( "        " ).append( XMLHandler.addTagValue( "field_c" , fieldsC[i] ) );
			retval.append( "      </field>" ).append(Const.CR);
		}

		retval.append( "    </fields>" ).append(Const.CR);

		return retval.toString();
	}

	/**
	 * This method is called by PDI when a step needs to load its configuration from XML.
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently read from the
	 * XML node passed in.
	 * 
	 * @param stepnode	the XML node containing the configuration
	 * @param databases	the databases available in the transformation
	 * @param metaStore the metaStore to optionally read from
	 */
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {

		try {
			Node fields = XMLHandler.getSubNode(stepnode, "fields");

			int nrfields = XMLHandler.countNodes( fields, "field" );
			allocate( nrfields);

			for ( int i = 0; i < nrfields; i++ ) {
				Node line = XMLHandler.getSubNodeByNr( fields, "field", i );
				outputFields[i] = Const.NVL(XMLHandler.getTagValue(line, "output_field"), "");
				fieldsA[i] = Const.NVL(XMLHandler.getTagValue(line, "field_a"), "");
				fieldsB[i] = Const.NVL(XMLHandler.getTagValue(line, "field_b"), "");
				fieldsC[i] = Const.NVL(XMLHandler.getTagValue(line, "field_c"), "");
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
	 * @param rep					the repository to save to
	 * @param metaStore				the metaStore to optionally write to
	 * @param id_transformation		the id to use for the transformation when saving
	 * @param id_step				the id to use for the step  when saving
	 */
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException
	{
		try{

			for ( int i = 0; i < outputFields.length; i++ ) {
				rep.saveStepAttribute( id_transformation, id_step, i, "output_field" , outputFields[i] );
				rep.saveStepAttribute( id_transformation, id_step, i, "field_a", fieldsA[i] );
				rep.saveStepAttribute( id_transformation, id_step, i, "field_b", fieldsB[i] );
				rep.saveStepAttribute( id_transformation, id_step, i, "field_c", fieldsC[i] );
			}
		}
		catch(Exception e){
			throw new KettleException("Unable to save step into repository: "+id_step, e); 
		}
	}		
	
	/**
	 * This method is called by PDI when a step needs to read its configuration from a repository.
	 * The repository implementation provides the necessary methods to read the step attributes.
	 * 
	 * @param rep		the repository to read from
	 * @param metaStore	the metaStore to optionally read from
	 * @param id_step	the id of the step being read
	 * @param databases	the databases available in the transformation
	 */
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException  {
		try{

			int nrFields = rep.countNrStepAttributes(id_step, getRepCode("output_field"));
			allocate( nrFields );

			for ( int i = 0; i < nrFields; i++ ) {
				outputFields[i] = rep.getStepAttributeString( id_step, i, getRepCode( "output_field" ) );
				fieldsA[i] = rep.getStepAttributeString( id_step, i, getRepCode( "field_a" ) );
				fieldsB[i] = rep.getStepAttributeString( id_step, i, getRepCode("field_b"));
				fieldsC[i] = rep.getStepAttributeString( id_step, i, getRepCode( "field_c" ) );
			}
		}
		catch(Exception e){
			throw new KettleException("Unable to load step from repository", e);
		}
	}

	/**
	 * This method is called to determine the changes the step is making to the row-stream.
	 * To that end a RowMetaInterface object is passed in, containing the row-stream structure as it is when entering
	 * the step. This method must apply any changes the step makes to the row stream. Usually a step adds fields to the
	 * row-stream.
	 * 
	 * @param inputRowMeta		the row structure coming in to the step
	 * @param name 				the name of the step making the changes
	 * @param info				row structures of any info steps coming in
	 * @param nextStep			the description of a step this step is passing rows to
	 * @param space				the variable space for resolving variables
	 * @param repository		the repository instance optionally read from
	 * @param metaStore			the metaStore to optionally read from
	 */
	public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
						  VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException{

		try {

			for(int i = 0; i < outputFields.length; i++){
				int valueType = retrieveNewFieldValueMeta(inputRowMeta, i);
				ValueMetaInterface v = ValueMetaFactory.createValueMeta(outputFields[i], valueType);

				v.setOrigin( name );
				inputRowMeta.addValueMeta(v);
			}

		} catch (Exception e){
			throw new KettleStepException(e);
		}
		
	}

	/**
	 * If all 3 fields are of the same data type then the output field should mirror this to avoid adding additional step to change type
	 * otherwise return a more generic String type
	 */
	private int retrieveNewFieldValueMeta(RowMetaInterface inputRowMeta, int rowIndex) throws Exception{

		Integer valueType;
		Integer typeOfA = null;
		Integer typeOfB = null;
		Integer typeOfC = null;

		int index = inputRowMeta.indexOfValue(fieldsA[rowIndex]);
		if(index != -1){
			typeOfA = inputRowMeta.getValueMeta(index).getType();
		}

		index = inputRowMeta.indexOfValue(fieldsB[rowIndex]);
		if(index != -1){
			typeOfB = inputRowMeta.getValueMeta(index).getType();
		}

		if((valueType = compareTwoTypes(typeOfA, typeOfB)) == STRING_AS_DEFAULT){
			return ValueMetaInterface.TYPE_STRING;
		}

		index = inputRowMeta.indexOfValue(fieldsC[rowIndex]);
		if(index != -1){
			typeOfC = inputRowMeta.getValueMeta(index).getType();
		}

		if((valueType = compareTwoTypes(valueType, typeOfC)) == STRING_AS_DEFAULT){
			return ValueMetaInterface.TYPE_STRING;
		} else {
			return valueType;
		}
	}

	private Integer compareTwoTypes(Integer a, Integer b) {
		if(a == null){
			return b;
		} else {
			if(b == null){
				return a;
			}
			return a.equals(b) ? a : STRING_AS_DEFAULT;
		}
	}

	/**
	 * This method is called when the user selects the "Verify Transformation" option in Spoon. 
	 * A list of remarks is passed in that this method should add to. Each remark is a comment, warning, error, or ok.
	 * The method should perform as many checks as necessary to catch design-time errors.
	 * 
	 * Typical checks include:
	 * - verify that all mandatory configuration is given
	 * - verify that the step receives any input, unless it's a row generating step
	 * - verify that the step does not receive any input if it does not take them into account
	 * - verify that the step finds fields it relies on in the row-stream
	 * 
	 *   @param remarks		the list of remarks to append to
	 *   @param transMeta	the description of the transformation
	 *   @param stepMeta	the description of the step
	 *   @param prev		the structure of the incoming row-stream
	 *   @param input		names of steps sending input to the step
	 *   @param output		names of steps this step is sending output to
	 *   @param info		fields coming in from info steps 
	 *   @param metaStore	metaStore to optionally read from
	 */
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
					  RowMetaInterface prev, String input[], String output[], RowMetaInterface info,
					  VariableSpace space, Repository repository, IMetaStore metaStore)  {
		
		CheckResult cr;

		// See if there are input streams leading to this step!
		if (input.length > 0) {
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
					BaseMessages.getString(PKG, "CoalesceMeta.CheckResult.ReceivingRows.OK"), stepMeta);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
					BaseMessages.getString(PKG, "CoalesceMeta.CheckResult.ReceivingRows.ERROR"), stepMeta);
		}
		remarks.add(cr);


		// See if there are missing input streams
		String errorMessage = "";
		for ( int i = 0; i < outputFields.length; i++ ) {

			ValueMetaInterface fA = prev.searchValueMeta( fieldsA[i] );
			ValueMetaInterface fB = prev.searchValueMeta( fieldsB[i] );
			ValueMetaInterface fC = prev.searchValueMeta( fieldsC[i] );

			String missingFields =
					  ((!fieldsA[i].isEmpty() && fA == null) ? fieldsA[i] + Const.CR : "")
					+ ((!fieldsB[i].isEmpty() && fB == null) ? fieldsB[i] + Const.CR : "")
					+ ((!fieldsC[i].isEmpty() && fC == null) ? fieldsC[i] + Const.CR : "");

			if ( !missingFields.isEmpty()) {
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

	void allocate( int nrFields ) {
		outputFields = new String[nrFields];
		fieldsA = new String[nrFields];
		fieldsB = new String[nrFields];
		fieldsC = new String[nrFields];
	}

}
