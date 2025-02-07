/**
 * Example data structures for "Users → Roles → Permission Sets → Permissions".
 * In a real system, these might come from a DB or from Admin config.
 */

// 1) Permission sets
const permissionSets = {
  adminSet: [
    'VIEW_QUEUE',
    'DELETE_QUEUE',
    'SCHEDULE_JOBS'
    // add others if needed
  ],
  readOnlySet: [
    'VIEW_QUEUE'
    // no delete, no schedule
  ]
};

// 2) Roles map to one permission set
const roles = {
  admin: 'adminSet',
  readOnly: 'readOnlySet'
};

// 3) Users map to a specific role
const users = {
  'jim@fortifiedstrength.org': 'admin'
  // unknown user defaults to read-only or an empty role
};

/**
 * Return the role name for a given user email
 */
function getRoleForUser(email) {
  return users[email] || 'readOnly';
}

/**
 * Return the permission set name for a given role
 */
function getPermissionSetForRole(roleName) {
  return roles[roleName] || 'readOnlySet';
}

/**
 * Return the list of permissions for a given permission set
 */
function getPermissionsForSet(setName) {
  return permissionSets[setName] || [];
}

/**
 * Return the complete list of permissions for a given user
 */
function getUserPermissions(email) {
  const role = getRoleForUser(email);
  const setName = getPermissionSetForRole(role);
  return getPermissionsForSet(setName);
}

/**
 * Return an object with { name: <roleName>, permissions: <string[]> }
 */
function getUserRole(email) {
  const roleName = getRoleForUser(email);
  const permSetName = getPermissionSetForRole(roleName);
  const perms = getPermissionsForSet(permSetName);
  return {
    name: roleName,
    permissions: perms
  };
}

/**
 * Return true if user has the given permission
 */
function userHasPermission(email, permission) {
  const perms = getUserPermissions(email);
  return perms.includes(permission);
}

// Helpers for specific checks
function userCanView(email) {
  return userHasPermission(email, 'VIEW_QUEUE');
}
function userCanDelete(email) {
  return userHasPermission(email, 'DELETE_QUEUE');
}
function userCanSchedule(email) {
  return userHasPermission(email, 'SCHEDULE_JOBS');
}
