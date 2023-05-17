/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.security.spi.credential;

import io.cdap.cdap.api.security.credential.CredentialIdentity;
import io.cdap.cdap.api.security.credential.CredentialProvisionerProfile;
import io.cdap.cdap.api.security.credential.CredentialProvisioningException;
import io.cdap.cdap.api.security.credential.IdentityValidationException;
import io.cdap.cdap.api.security.credential.ProvisionedCredential;

/**
 * Defines a contract for provisioning a credential.
 */
public interface CredentialProvisioner {

  /**
   * @return the name of the credential provisioner implementation.
   */
  String getName();

  /**
   * Provisions a short-lived credential for the provided identity using the
   * provided provisioner profile.
   */
  ProvisionedCredential provision(CredentialProvisionerProfile profile, CredentialIdentity identity)
      throws CredentialProvisioningException;

  /**
   * Validates the provided identity.
   */
  void validateIdentity(CredentialProvisionerProfile profile, CredentialIdentity identity)
      throws IdentityValidationException;
}
