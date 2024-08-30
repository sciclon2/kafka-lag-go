package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFetchConsumerGroups(t *testing.T) {
	mockAdmin := new(MockSaramaClusterAdmin)
	groupChan := setupMockAdminAndGroupChan(mockAdmin, map[string]string{
		"group-1": "",
		"group-2": "",
		"group-3": "",
		"group-4": "",
	}, 4)

	conf := createConfig("group-(1|3|4)", "group-2")

	FetchConsumerGroups(mockAdmin, groupChan, conf)

	var resultGroups []string
	for group := range groupChan {
		resultGroups = append(resultGroups, group)
	}

	expectedGroups := []string{"group-1", "group-3", "group-4"}
	assert.ElementsMatch(t, expectedGroups, resultGroups)

	mockAdmin.AssertExpectations(t)
}

func TestFetchConsumerGroups_NoGroups(t *testing.T) {
	mockAdmin := new(MockSaramaClusterAdmin)
	groupChan := setupMockAdminAndGroupChan(mockAdmin, map[string]string{}, 1)

	conf := createConfig("", "")

	FetchConsumerGroups(mockAdmin, groupChan, conf)

	var resultGroups []string
	for group := range groupChan {
		resultGroups = append(resultGroups, group)
	}

	assert.Empty(t, resultGroups)

	mockAdmin.AssertExpectations(t)
}

func TestFetchConsumerGroups_AllBlacklisted(t *testing.T) {
	mockAdmin := new(MockSaramaClusterAdmin)
	groupChan := setupMockAdminAndGroupChan(mockAdmin, map[string]string{
		"group-1": "",
		"group-2": "",
		"group-3": "",
	}, 3)

	conf := createConfig("", "group-.*")

	FetchConsumerGroups(mockAdmin, groupChan, conf)

	var resultGroups []string
	for group := range groupChan {
		resultGroups = append(resultGroups, group)
	}

	assert.Empty(t, resultGroups)

	mockAdmin.AssertExpectations(t)
}

func TestFetchConsumerGroups_NotInWhitelist(t *testing.T) {
	mockAdmin := new(MockSaramaClusterAdmin)
	groupChan := setupMockAdminAndGroupChan(mockAdmin, map[string]string{
		"group-1": "",
		"group-2": "",
		"group-3": "",
	}, 3)

	conf := createConfig("group-(1|3)", "")

	FetchConsumerGroups(mockAdmin, groupChan, conf)

	var resultGroups []string
	for group := range groupChan {
		resultGroups = append(resultGroups, group)
	}

	expectedGroups := []string{"group-1", "group-3"}
	assert.ElementsMatch(t, expectedGroups, resultGroups)

	mockAdmin.AssertExpectations(t)
}

func TestFetchConsumerGroups_WhitelistAndBlacklist(t *testing.T) {
	mockAdmin := new(MockSaramaClusterAdmin)
	groupChan := setupMockAdminAndGroupChan(mockAdmin, map[string]string{
		"group-1": "",
		"group-2": "",
		"group-3": "",
		"group-4": "",
	}, 4)

	conf := createConfig("group-(1|3|4)", "group-3")

	FetchConsumerGroups(mockAdmin, groupChan, conf)

	var resultGroups []string
	for group := range groupChan {
		resultGroups = append(resultGroups, group)
	}

	expectedGroups := []string{"group-1", "group-3", "group-4"}
	assert.ElementsMatch(t, expectedGroups, resultGroups)

	mockAdmin.AssertExpectations(t)
}
