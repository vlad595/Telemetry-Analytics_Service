using System;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;

namespace Services
{
    public interface IUserSubscriptionHandler
    {
        public Task<int> GetSubscriptionTier(string agentId);
    }
    public class UserSubscriptionHandler : IUserSubscriptionHandler
    {
        private readonly IConfiguration _config;
        private readonly IMemoryCache _cache;
        private static readonly TimeSpan CacheDuration = TimeSpan.FromDays(1);
        static HttpClient httpClient = new HttpClient();
        public UserSubscriptionHandler(IConfiguration config, IMemoryCache cache)
        {
            _config = config;
            _cache = cache;
        }
        public async Task<int> GetSubscriptionTier(string agentId)
        {
            if (_cache.TryGetValue(agentId, out int cachedTier))
            {
                return cachedTier;
            }

            string authApiUrl = _config["AuthApiUrl"] ?? throw new InvalidOperationException("AuthApiUrl is not configured");
            string requestUrl = $"{authApiUrl.TrimEnd('/')}/api/user/instance={Uri.EscapeDataString(agentId)}";
            using HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
            using HttpResponseMessage response = await httpClient.SendAsync(request);

            if (response.StatusCode == System.Net.HttpStatusCode.OK)
            {
                string content = await response.Content.ReadAsStringAsync();
                using JsonDocument document = JsonDocument.Parse(content);
                if (!TryGetPropertyIgnoreCase(document.RootElement, "subscriptionLevel", out JsonElement subscriptionLevel))
                {
                    throw new JsonException("Subscription level was not found in the auth API response");
                }

                int tierValue = subscriptionLevel.ValueKind switch
                {
                    JsonValueKind.Number when subscriptionLevel.TryGetInt32(out int number) => number,
                    JsonValueKind.String when int.TryParse(subscriptionLevel.GetString(), out int number) => number,
                    _ => throw new JsonException("Subscription level must be an integer")
                };

                int subscriptionTier = tierValue + 1;
                _cache.Set(agentId, subscriptionTier, CacheDuration);
                return subscriptionTier;
            }
            return 3;
        }

        private static bool TryGetPropertyIgnoreCase(JsonElement element, string propertyName, out JsonElement value)
        {
            foreach (JsonProperty property in element.EnumerateObject())
            {
                if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }
    }
}
