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

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;

import java.util.*;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI.
 *
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *
 * This class is the implementation of StepDialogInterface.
 * Classes implementing this interface need to:
 *
 * - build and open a SWT dialog displaying the step's settings (stored in the step's meta object)
 * - write back any changes the user makes to the step's meta object
 * - report whether the user changed any settings when confirming the dialog
 *
 */
public class CoalesceDialog extends BaseStepDialog implements StepDialogInterface {

	/**
	 * The PKG member is used when looking up internationalized strings.
	 * The properties file with localized keys is expected to reside in
	 * {the package of the class specified}/com.graphiq.pdi.coalesce.messages/messages_{locale}.properties
	 */
	private static Class<?> PKG = CoalesceMeta.class; // for i18n purposes

	// this is the object the stores the step's settings
	// the dialog reads the settings from it when opening
	// the dialog writes the settings to it when confirmed 
	private CoalesceMeta meta;

	private TableView wFields;
	private ColumnInfo[] columnInfos;

	private Map<String, Integer> allInputStreamFields;

	/**
	 * Constants:
	 */
	public static final String HELP_DOCUMENTATION_URL =
					"https://github.com/graphiq-data/pdi-coalesce-plugin/blob/master/help.md";

	/**
	 * The constructor should simply invoke super() and save the incoming meta
	 * object to a local variable, so it can conveniently read and write settings
	 * from/to it.
	 *
	 * @param parent    the SWT shell to open the dialog in
	 * @param in        the meta object holding the step's settings
	 * @param transMeta transformation description
	 * @param sname     the step name
	 */
	public CoalesceDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
		super( parent, (BaseStepMeta) in, transMeta, sname );
		meta = (CoalesceMeta) in;

		allInputStreamFields = new HashMap<String, Integer>();
	}

	/**
	 * This method is called by Spoon when the user opens the settings dialog of the step.
	 * It should open the dialog and return only once the dialog has been closed by the user.
	 *
	 * If the user confirms the dialog, the meta object (passed in the constructor) must
	 * be updated to reflect the new step settings. The changed flag of the meta object must
	 * reflect whether the step configuration was changed by the dialog.
	 *
	 * If the user cancels the dialog, the meta object must not be updated, and its changed flag
	 * must remain unaltered.
	 *
	 * The open() method must return the name of the step after the user has confirmed the dialog,
	 * or null if the user cancelled the dialog.
	 */
	public String open() {

		// store some convenient SWT variables 
		Shell parent = getParent();
		Display display = parent.getDisplay();

		// SWT code for preparing the dialog
		shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
		props.setLook( shell );
		setShellImage( shell, meta );

		// Save the value of the changed flag on the meta object. If the user cancels
		// the dialog, it will be restored to this saved value.
		// The "changed" variable is inherited from BaseStepDialog
		changed = meta.hasChanged();

		// The ModifyListener used on all controls. It will update the meta object to 
		// indicate that changes are being made.
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText( ModifyEvent e ) {
				meta.setChanged();
			}
		};

		// ------------------------------------------------------- //
		// SWT code for building the actual settings dialog        //
		// ------------------------------------------------------- //
		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout( formLayout );
		shell.setText( BaseMessages.getString( PKG, "CoalesceDialog.Shell.Title" ) );

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		setStepName( middle, margin, lsMod );

		// Column infos
		setTable( margin, lsMod );

		// OK and cancel buttons
		setBottomButtons( margin );

		// default listener (for hitting "enter")
		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected( SelectionEvent e ) {
				ok();
			}
		};
		wStepname.addSelectionListener( lsDef );

		// Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
		shell.addShellListener( new ShellAdapter() {
			public void shellClosed( ShellEvent e ) {
				cancel();
			}
		} );

		// Set/Restore the dialog size based on last position on screen
		// The setSize() method is inherited from BaseStepDialog
		setSize();

		// populate the dialog with the values from the meta object
		populateDialog();

		// restore the changed flag to original value, as the modify listeners fire during dialog population 
		meta.setChanged( changed );

		// open dialog
		shell.open();

		return stepname;
	}

	@Override
	protected Button createHelpButton( Shell shell, StepMeta stepMeta, PluginInterface plugin ) {
		plugin.setDocumentationUrl( HELP_DOCUMENTATION_URL );
		return super.createHelpButton( shell, stepMeta, plugin );
	}

	/**
	 * This helper method puts the step configuration stored in the meta object
	 * and puts it into the dialog controls.
	 */
	private void populateDialog() {
		if ( meta.getOutputFields() != null ) {
			for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
				TableItem item = wFields.table.getItem( i );
				if ( meta.getOutputFields()[i] != null ) {
					item.setText( 1, meta.getOutputFields()[i] );
				}
				for ( int j = 0; j < CoalesceMeta.noInputFields; j++ ) {
					if ( meta.getInputFields()[i][j] != null ) {
						item.setText( j + 2, meta.getInputFields()[i][j] );
					}
				}
				item.setText( 2 + CoalesceMeta.noInputFields, ValueMeta.getTypeDesc( meta.getValueType()[i] ) );
				item.setText( 3 + CoalesceMeta.noInputFields, CoalesceMeta.getStringFromBoolean( meta.getDoRemoveInputFields()[i] ) );
			}
		}
		wFields.setRowNums();
		wFields.optWidth( true );

		wStepname.selectAll();
		wStepname.setFocus();
	}

	/**
	 * Called when the user cancels the dialog.
	 */
	private void cancel() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to null to indicate that dialog was cancelled.
		stepname = null;
		// Restoring original "changed" flag on the met aobject
		meta.setChanged( changed );
		// close the SWT dialog window
		dispose();
	}

	/**
	 * Called when the user confirms the dialog
	 */
	private void ok() {
		stepname = wStepname.getText();
		if ( Const.isEmpty( stepname ) ) {
			return;
		}

		populateMetaWithInfo();
		// close the SWT dialog window
		dispose();
	}

	private void populateMetaWithInfo() {
		int nrkeys = wFields.nrNonEmpty();

		meta.allocate( nrkeys );
		if ( log.isDebug() ) {
			logDebug( BaseMessages.getString( PKG, "CoalesceDialog.Log.FoundFields", String.valueOf( nrkeys ) ) );
		}

		//CHECKSTYLE:Indentation:OFF
		for ( int i = 0; i < nrkeys; i++ ) {
			TableItem item = wFields.getNonEmpty( i );
			meta.getOutputFields()[i] = item.getText( 1 );

			int noNonEmptyFields = 0;
			for ( int j = 0; j < CoalesceMeta.noInputFields; j++ ) {
				meta.getInputFields()[i][j] = item.getText( 2 + j );

				if ( !meta.getInputFields()[i][j].isEmpty() ) {
					noNonEmptyFields++;
				}
			}

			if ( noNonEmptyFields < 2 ) {
				MessageDialogWithToggle md =
								new MessageDialogWithToggle(
												shell,
												BaseMessages.getString( PKG, "CoalesceDialog.Validations.DialogTitle" ),
												null,
												BaseMessages.getString( PKG, "CoalesceDialog.Validations.DialogMessage", Const.CR, Const.CR ),
												MessageDialog.WARNING,
												new String[] { BaseMessages.getString( PKG, "CoalesceDialog.Validations.Option.1" ) },
												0,
												BaseMessages.getString( PKG, "CoalesceDialog.Validations.Option.2" ),
												false );
				MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
				md.open();
			}

			String typeValueText = item.getText( 2 + CoalesceMeta.noInputFields );
			meta.getValueType()[i] = typeValueText.isEmpty() ? ValueMeta.TYPE_NONE
							: ValueMeta.getType( typeValueText );

			String isRemoveText = item.getText( 3 + CoalesceMeta.noInputFields );
			meta.getDoRemoveInputFields()[i] = isRemoveText.isEmpty() ? false
							: CoalesceMeta.getBooleanFromString( isRemoveText );
		}
	}

	private void setStepName( int middle, int margin, ModifyListener lsMod ) {
		wlStepname = new Label( shell, SWT.RIGHT );
		wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
		props.setLook( wlStepname );
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment( 0, 0 );
		fdlStepname.right = new FormAttachment( middle, -margin );
		fdlStepname.top = new FormAttachment( 0, margin );
		wlStepname.setLayoutData( fdlStepname );

		wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		wStepname.setText( stepname );
		props.setLook( wStepname );
		wStepname.addModifyListener( lsMod );
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment( middle, 0 );
		fdStepname.top = new FormAttachment( 0, margin );
		fdStepname.right = new FormAttachment( 100, 0 );
		wStepname.setLayoutData( fdStepname );
	}


	private void setTable( int margin, ModifyListener lsMod ) {
		Label wlFields = new Label( shell, SWT.NONE );
		wlFields.setText( BaseMessages.getString( PKG, "CoalesceDialog.Fields.Label" ) );
		props.setLook( wlFields );
		FormData fdlFields = new FormData();
		fdlFields.left = new FormAttachment( 0, 0 );
		fdlFields.top = new FormAttachment( wStepname, margin );
		wlFields.setLayoutData( fdlFields );

		columnInfos = new ColumnInfo[3 + CoalesceMeta.noInputFields];
		columnInfos[0] = new ColumnInfo( BaseMessages.getString( PKG, "CoalesceDialog.ColumnInfo.OutField" ),
						ColumnInfo.COLUMN_TYPE_TEXT, false );
		for ( int i = 0; i < CoalesceMeta.noInputFields; i++ ) {
			columnInfos[i + 1] = new ColumnInfo(
							BaseMessages.getString( PKG, "CoalesceDialog.ColumnInfo.InputField",
											Character.valueOf( (char) ( 'A' + i ) ).toString() ),
							ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
		}
		columnInfos[1 + CoalesceMeta.noInputFields] = new ColumnInfo(
						BaseMessages.getString( PKG, "CoalesceDialog.ColumnInfo.ValueType" ),
						ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes() );
		columnInfos[2 + CoalesceMeta.noInputFields] = new ColumnInfo(
						BaseMessages.getString( PKG, "CoalesceDialog.ColumnInfo.RemoveInputColumns" ),
						ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
						BaseMessages.getString( PKG, "System.Combo.No" ),
						BaseMessages.getString( PKG, "System.Combo.Yes" ) } );

		columnInfos[2 + CoalesceMeta.noInputFields].setToolTip( BaseMessages.getString
						( PKG, "CoalesceDialog.ColumnInfo.RemoveInputColumns.Tooltip" ) );

		int noFieldRows = ( meta.getOutputFields() != null ? meta.getOutputFields().length : 1 );
		wFields = new TableView( transMeta, shell,
						SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnInfos, noFieldRows, lsMod, props );

		FormData fdFields = new FormData();
		fdFields.left = new FormAttachment( 0, 0 );
		fdFields.top = new FormAttachment( wlFields, margin );
		fdFields.right = new FormAttachment( 100, 0 );
		fdFields.bottom = new FormAttachment( 100, -50 );
		wFields.setLayoutData( fdFields );

		final Runnable runnable = new Runnable() {
			public void run() {
				StepMeta stepMeta = transMeta.findStep( stepname );
				if ( stepMeta != null ) {
					try {
						RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

						// Remember these fields...
						for ( int i = 0; i < row.size(); i++ ) {
							allInputStreamFields.put( row.getValueMeta( i ).getName(), i );
						}

						setComboBoxes();
					} catch ( KettleException e ) {
						logError( BaseMessages.getString( PKG, "CoalesceDialog.Log.UnableToFindInput" ) );
					}
				}
			}
		};
		new Thread( runnable ).start();

		wFields.addModifyListener( new ModifyListener() {
			public void modifyText( ModifyEvent arg0 ) {
				// Now set the combo's
				shell.getDisplay().asyncExec( new Runnable() {
					public void run() {
						setComboBoxes();
					}
				} );
			}
		} );
	}

	private void setComboBoxes() {
		// Something was changed in the row.
		final Map<String, Integer> fields = new TreeMap<String, Integer>(allInputStreamFields);

		String[] fieldNames = new String[allInputStreamFields.size()];
		fieldNames = fields.keySet().toArray( fieldNames );

		for ( int i = 0; i < CoalesceMeta.noInputFields; i++ ) {
			columnInfos[1 + i].setComboValues( fieldNames );
		}
	}

	private void setBottomButtons( int margin ) {
		wOK = new Button( shell, SWT.PUSH );
		wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
		wCancel = new Button( shell, SWT.PUSH );
		wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

		BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

		// Add listeners for cancel and OK
		lsCancel = new Listener() {
			public void handleEvent( Event e ) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent( Event e ) {
				ok();
			}
		};

		wCancel.addListener( SWT.Selection, lsCancel );
		wOK.addListener( SWT.Selection, lsOK );
	}
}
