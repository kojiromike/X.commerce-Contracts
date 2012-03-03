/**
 * <copyright>
 * </copyright>
 *
 */
package com.x.xocl.domainmodel.impl;

import com.x.xocl.domainmodel.BranchCondition;
import com.x.xocl.domainmodel.DomainmodelPackage;
import com.x.xocl.domainmodel.WorkflowBlock;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Branch Condition</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * <ul>
 *   <li>{@link com.x.xocl.domainmodel.impl.BranchConditionImpl#getContents <em>Contents</em>}</li>
 * </ul>
 * </p>
 *
 * @generated
 */
public class BranchConditionImpl extends MinimalEObjectImpl.Container implements BranchCondition
{
  /**
   * The cached value of the '{@link #getContents() <em>Contents</em>}' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getContents()
   * @generated
   * @ordered
   */
  protected WorkflowBlock contents;

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  protected BranchConditionImpl()
  {
    super();
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  protected EClass eStaticClass()
  {
    return DomainmodelPackage.Literals.BRANCH_CONDITION;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public WorkflowBlock getContents()
  {
    return contents;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public NotificationChain basicSetContents(WorkflowBlock newContents, NotificationChain msgs)
  {
    WorkflowBlock oldContents = contents;
    contents = newContents;
    if (eNotificationRequired())
    {
      ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, DomainmodelPackage.BRANCH_CONDITION__CONTENTS, oldContents, newContents);
      if (msgs == null) msgs = notification; else msgs.add(notification);
    }
    return msgs;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public void setContents(WorkflowBlock newContents)
  {
    if (newContents != contents)
    {
      NotificationChain msgs = null;
      if (contents != null)
        msgs = ((InternalEObject)contents).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - DomainmodelPackage.BRANCH_CONDITION__CONTENTS, null, msgs);
      if (newContents != null)
        msgs = ((InternalEObject)newContents).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - DomainmodelPackage.BRANCH_CONDITION__CONTENTS, null, msgs);
      msgs = basicSetContents(newContents, msgs);
      if (msgs != null) msgs.dispatch();
    }
    else if (eNotificationRequired())
      eNotify(new ENotificationImpl(this, Notification.SET, DomainmodelPackage.BRANCH_CONDITION__CONTENTS, newContents, newContents));
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
  {
    switch (featureID)
    {
      case DomainmodelPackage.BRANCH_CONDITION__CONTENTS:
        return basicSetContents(null, msgs);
    }
    return super.eInverseRemove(otherEnd, featureID, msgs);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public Object eGet(int featureID, boolean resolve, boolean coreType)
  {
    switch (featureID)
    {
      case DomainmodelPackage.BRANCH_CONDITION__CONTENTS:
        return getContents();
    }
    return super.eGet(featureID, resolve, coreType);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public void eSet(int featureID, Object newValue)
  {
    switch (featureID)
    {
      case DomainmodelPackage.BRANCH_CONDITION__CONTENTS:
        setContents((WorkflowBlock)newValue);
        return;
    }
    super.eSet(featureID, newValue);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public void eUnset(int featureID)
  {
    switch (featureID)
    {
      case DomainmodelPackage.BRANCH_CONDITION__CONTENTS:
        setContents((WorkflowBlock)null);
        return;
    }
    super.eUnset(featureID);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public boolean eIsSet(int featureID)
  {
    switch (featureID)
    {
      case DomainmodelPackage.BRANCH_CONDITION__CONTENTS:
        return contents != null;
    }
    return super.eIsSet(featureID);
  }

} //BranchConditionImpl
