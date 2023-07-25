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

package io.cdap.cdap.internal.credential;

import com.google.common.util.concurrent.AbstractIdleService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.credential.CredentialProvider;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Default implementation for {@link CredentialProviderService} used in AppFabric.
 */
public class DefaultCredentialProviderService extends AbstractIdleService
    implements CredentialProviderService {

  private final CConfiguration cConf;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final Map<String, CredentialProvider> credentialProviders;
  private final CredentialIdentityManager credentialIdentityManager;
  private final CredentialProfileManager credentialProfileManager;

  @Inject
  DefaultCredentialProviderService(CConfiguration cConf,
      ContextAccessEnforcer contextAccessEnforcer,
      CredentialProviderLoader credentialProviderLoader,
      CredentialIdentityManager credentialIdentityManager,
      CredentialProfileManager credentialProfileManager) {
    this.cConf = cConf;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.credentialProviders = credentialProviderLoader.loadCredentialProviders();
    this.credentialIdentityManager = credentialIdentityManager;
    this.credentialProfileManager = credentialProfileManager;
  }

  @Override
  protected void startUp() throws Exception {
    for (CredentialProvider provider : credentialProviders.values()) {
      provider.initialize(new DefaultCredentialProviderContext(cConf, provider.getName()));
    }
  }

  @Override
  protected void shutDown() throws Exception {

  }

  /**
   * Provisions a credential.
   *
   * @param namespace    The identity namespace.
   * @param identityName The identity name.
   * @return A provisioned credential.
   * @throws CredentialProvisioningException If provisioning fails in the extension.
   * @throws IOException                     If any transport errors occur.
   * @throws NotFoundException               If the identity or profile are not found.
   */
  @Override
  public ProvisionedCredential provision(String namespace, String identityName)
      throws CredentialProvisioningException, IOException, NotFoundException {
    CredentialIdentityId identityId = new CredentialIdentityId(namespace, identityName);
    contextAccessEnforcer.enforce(identityId, StandardPermission.USE);
    Optional<CredentialIdentity> optIdentity = credentialIdentityManager.get(identityId);
    if (!optIdentity.isPresent()) {
      throw new NotFoundException(String.format("Credential identity '%s' was not found.",
          identityId.toString()));
    }
    CredentialIdentity identity = optIdentity.get();
    return validateAndProvisionIdentity(identity);
  }

  /**
   * Validates an identity.
   *
   * @param identity The identity to validate.
   * @throws IdentityValidationException If identity validation fails in the extension.
   * @throws IOException                 If any transport errors occur.
   * @throws NotFoundException           If the identity or profile are not found.
   */
  @Override
  public void validateIdentity(CredentialIdentity identity) throws IdentityValidationException,
      IOException, NotFoundException {
    try {
      validateAndProvisionIdentity(identity);
    } catch (CredentialProvisioningException e) {
      throw new IdentityValidationException(e);
    }
  }

  private ProvisionedCredential validateAndProvisionIdentity(CredentialIdentity identity)
      throws CredentialProvisioningException, IOException, NotFoundException {
    CredentialProfileId profileId = new CredentialProfileId(identity.getProfileNamespace(),
        identity.getProfileName());
    contextAccessEnforcer.enforce(profileId, StandardPermission.USE);
    Optional<CredentialProfile> optProfile = credentialProfileManager.get(profileId);
    if (!optProfile.isPresent()) {
      throw new NotFoundException(String.format("Credential profile '%s' was not found.",
          profileId.toString()));
    }
    CredentialProfile profile = optProfile.get();
    // This is a sanity check which should be impossible to fail.
    String providerType = profile.getCredentialProviderType();
    if (!credentialProviders.containsKey(providerType)) {
      throw new IllegalStateException(String.format("Unsupported credential provider type "
          + "'%'", providerType));
    }
    // Provision and return the credential.
    return credentialProviders.get(providerType).provision(profile, identity);
  }
}