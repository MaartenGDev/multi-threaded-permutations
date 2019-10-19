FROM mcr.microsoft.com/dotnet/core/runtime:2.2

COPY MultiThreadedPermutations/bin/Release/netcoreapp2.2/osx-x64/publish/ app/

ENTRYPOINT ["dotnet", "app/myapp.dll", "abcdefghij"]