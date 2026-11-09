FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Telemetry-AnalyticsService.csproj", "./"]
RUN dotnet restore "Telemetry-AnalyticsService.csproj"

COPY . .
RUN dotnet publish "Telemetry-AnalyticsService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.Microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Telemetry-AnalyticsService.dll"]
