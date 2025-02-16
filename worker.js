const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });

let expirationDate;


const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date
    ? {
        filters: [
          { propertyName, operator: 'GT', value: date.valueOf() },
          { propertyName, operator: 'LT', value: nowDate.valueOf() }
        ]
      }
    : {};

  return lastModifiedDateFilter;
};


const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult || !searchResult.results || searchResult.results.length === 0) {
      throw new Error('Failed to fetch companies for the 4th time. Aborting.');
    }

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');   
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        osearchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult || !searchResult.results || searchResult.results.length === 0) {
      throw new Error('Failed to fetch contacts after 4 attempts. Aborting.');
    }
    
    const data = searchResult.results || [];

    console.log('fetch contact batch');

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

const processMeetings = async (domain, hubId, q) => {
  console.log(`Starting processMeetings for HubSpot Account: ${hubId}`);
  
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.meetings || '2000-01-01');
  const now = new Date();
  
  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    console.log("Fetching meetings from HubSpot...");
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'hs_lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: ['hs_meeting_title', 'hs_timestamp', 'createdAt', 'updatedAt'],
      limit,
      after: offsetObject.after
    };
    
    let searchResult = {};
    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        console.error(`Error fetching meetings (attempt ${tryCount + 1}):`, err);
        tryCount++;
        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
        await new Promise(resolve => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }
    
    if (!searchResult || !searchResult.results || searchResult.results.length === 0) {
      console.warn('No meetings found after retries. Skipping.');
      return;
    }
    
    console.log(`Retrieved ${searchResult.results.length} meetings.`);
    const data = searchResult.results || [];
    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    
    for (const meeting of data) {
      if (!meeting.properties) continue;
      
      const isCreated = new Date(meeting.createdAt) > lastPulledDate;
      const actionTemplate = {
        includeInAnalytics: 0,
        meetingProperties: {
          meeting_id: meeting.id,
          title: meeting.properties.hs_meeting_title,
          timestamp: meeting.properties.hs_timestamp,
          createdate: meeting.properties.hs_createdate,
        }
      };
      
      console.log(`Fetching attendees for meeting ${meeting.id}`);
      const contactEmails = await fetchMeetingAttendees(meeting.id);
      
      if (contactEmails.length === 0) {
        console.warn(`⚠️ No attendees found for meeting ${meeting.id}`);
      }
      
      for (const email of contactEmails) {
        console.log(`Queuing action for attendee: ${email}`);
        q.push({
          actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
          actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
          identity: email,
          ...actionTemplate
        });
      }
    }
    
    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  console.log(`Meetings processed successfully for HubSpot Account: ${hubId}`);
  account.lastPulledDates.meetings = now;
  await saveDomain(domain);
};


const fetchMeetingAttendees = async (meetingId) => {
  try {
    console.log(`Fetching attendee details for meeting ${meetingId}`);
    const contactIds = await fetchMeetingAssociations(meetingId);
    if (contactIds.length === 0) {
      console.warn(`⚠️ No attendees found for meeting ${meetingId}`);
      return [];
    }
    console.log(`Found ${contactIds.length} attendees for meeting ${meetingId}`);
    return await fetchContactDetails(contactIds);
  } catch (error) {
    console.error(`Error fetching attendees for meeting ${meetingId}:`, error);
    return [];
  }
};

const fetchMeetingAssociations = async (meetingId) => {
  try {
    console.log(`Fetching associated contacts for meeting ${meetingId}`);
    const response = await hubspotClient.apiRequest({
      method: 'get',
      path: `/crm/v3/objects/meetings/${meetingId}/associations/contacts`
    });
    const data = await response.json();
    return data.results?.map(contact => contact.id) || [];
  } catch (error) {
    console.error(`Error fetching associations for meeting ${meetingId}:`, error);
    return [];
  }
};

const fetchContactDetails = async (contactIds) => {
  try {
    console.log(`🔄 Fetching contact details for ${contactIds.length} attendees`);
    const response = await hubspotClient.apiRequest({
      method: 'post',
      path: `/crm/v3/objects/contacts/batch/read`,
      body: {
        properties: ['email', 'firstname', 'lastname'],
        inputs: contactIds.map(id => ({ id }))
      }
    });
    const contacts = await response.json();
    return contacts.results?.map(contact => contact.properties.email).filter(Boolean) || [];
  } catch (error) {
    console.error('❌ Error fetching contact details:', error);
    return [];
  }
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process meetings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
