import React, { useState, useEffect } from 'react';
import { Calendar, Loader2 } from 'lucide-react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Calendar as CalendarComponent } from "@/components/ui/calendar";
import { Label } from "@/components/ui/label";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";

// Types for our state
interface Parameter {
  key: string;
  value: string;
}

const SpotStatsApp = () => {
  // State management
  const [resources, setResources] = useState(['ALL']);
  const [selectedResource, setSelectedResource] = useState('ALL');
  const [startDate, setStartDate] = useState<Date | null>(null);
  const [endDate, setEndDate] = useState<Date | null>(null);
  const [endpoint, setEndpoint] = useState('');
  const [parameters, setParameters] = useState<Parameter[]>([{ key: '', value: '' }]);
  const [countries, setCountries] = useState(['ALL']);
  const [selectedCountry, setSelectedCountry] = useState('ALL');
  const [ontologyId, setOntologyId] = useState('');
  
  // Loading and error states
  const [isLoadingResources, setIsLoadingResources] = useState(true);
  const [isLoadingCountries, setIsLoadingCountries] = useState(true);
  const [isSearching, setIsSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // Results state
  interface SearchResults {
    resource: string;
    matching_requests: number;
  }
  
  const [searchResults, setSearchResults] = useState<SearchResults | null>(null);

  // Check if OLS is selected (case insensitive)
  const isOlsSelected = selectedResource.toUpperCase() === 'OLS';
  const hasOntologyId = ontologyId.trim() !== '';

  useEffect(() => {
    fetchResources();
    fetchCountries();
  }, []);

  const fetchCountries = async () => {
    try {
      const response = await fetch('/api/countries');
      if (!response.ok) throw new Error('Failed to fetch countries');
      let data = await response.json();
      // add an "All" option to the resources
      data = ['ALL', ...data];
      setCountries(data);
    } catch (err) {
      setError('Failed to load countries. Please try again later.');
    } finally {
      setIsLoadingCountries(false);
    }
  };

  const fetchResources = async () => {
    try {
      const response = await fetch('/api/resources');
      if (!response.ok) throw new Error('Failed to fetch resources');
      let data = await response.json();
      // add an "All" option to the resources
      data = ['ALL', ...data];
      setResources(data);
    } catch (err) {
      setError('Failed to load resources. Please try again later.');
    } finally {
      setIsLoadingResources(false);
    }
  };

  const handleSearch = async () => {
    if (!selectedResource) {
      setError('Please select a resource');
      return;
    }

    setError(null);
    setIsSearching(true);
    setSearchResults(null);

    try {
      // Build query parameters
      const queryParams = new URLSearchParams({
        resource_name: selectedResource,
      });

      if (startDate) {
        queryParams.append('start_date', startDate.toISOString().split('T')[0]);
      }
      if (endDate) {
        queryParams.append('end_date', endDate.toISOString().split('T')[0]);
      }
      // Handle OLS with ontology id - search for URLs containing the ontology id
      if (isOlsSelected && hasOntologyId) {
        queryParams.append('ontologyId', ontologyId.toLowerCase());
      } else if (endpoint) {
        queryParams.append('endpoint', endpoint);
      }
      if (selectedCountry !== 'ALL') {
        queryParams.append('country', selectedCountry);
      }

      // Filter out empty parameters and format as JSON
      const validParams = parameters.filter(p => p.key && p.value);
      if (validParams.length > 0) {
        const paramsObject = Object.fromEntries(
          validParams.map(p => [p.key, p.value])
        );
        queryParams.append('parameters', JSON.stringify(paramsObject));
      }

      // Create AbortController for 15 minute timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 900000); // 15 minutes
      
      const response = await fetch(`/api/stats/search?${queryParams}`, {
        signal: controller.signal,
        headers: {
          'Content-Type': 'application/json',
        }
      });
      
      clearTimeout(timeoutId);
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Search failed');
      }
      
      const data = await response.json();
      setSearchResults(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setIsSearching(false);
    }
  };

  const formatDate = (date: Date) => {
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    });
  };

  const addParameter = () => {
    setParameters([...parameters, { key: '', value: '' }]);
  };

  const updateParameter = (index: number, field: 'key' | 'value', value: string) => {
    const newParameters = [...parameters];
    newParameters[index][field] = value;
    setParameters(newParameters);
  };

  const removeParameter = (index: number) => {
    const newParameters = parameters.filter((_, i) => i !== index);
    setParameters(newParameters);
  };

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="container mx-auto px-4">
        <h1 className="text-3xl font-bold text-center text-gray-800 mb-8">
          SPOT Apps Stats
        </h1>

        <div className="grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Query Statistics</CardTitle>
              <CardDescription>
                Search through application usage statistics
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Resource Selection */}
              <div className="space-y-2">
                <Label htmlFor="resource">Resource</Label>
                <Select
                  value={selectedResource}
                  onValueChange={setSelectedResource}
                  disabled={isLoadingResources}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select a resource" />
                  </SelectTrigger>
                  <SelectContent>
                    {resources.map((resource) => (
                      <SelectItem key={resource} value={resource}>
                        {resource === 'ALL' ? 'All Resources' : resource}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Ontology ID - Only show when OLS is selected */}
              {isOlsSelected && (
                <div className="space-y-2">
                  <Label htmlFor="ontologyId">Ontology Id</Label>
                  <Input
                    id="ontologyId"
                    value={ontologyId}
                    onChange={(e) => setOntologyId(e.target.value)}
                    placeholder="Enter ontology id (e.g., efo)"
                  />
                </div>
              )}

              {/* Country Selection */}
              <div className="space-y-2">
                <Label htmlFor="country">Country</Label>
                <Select
                  value={selectedCountry}
                  onValueChange={setSelectedCountry}
                  disabled={isLoadingCountries}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select a country" />
                  </SelectTrigger>
                  <SelectContent>
                    {countries.map((country) => (
                      <SelectItem key={country} value={country}>
                        {country === 'ALL' ? 'All Countries' : country}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Date Range Selection */}
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>Start Date</Label>
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button
                        variant="outline"
                        className="w-full justify-start text-left font-normal"
                      >
                        <Calendar className="mr-2 h-4 w-4" />
                        {startDate ? formatDate(startDate) : "Select date"}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0" align="start">
                      <CalendarComponent
                        mode="single"
                        selected={startDate || undefined}
                        onSelect={(date) => setStartDate(date || null)}
                        initialFocus
                      />
                    </PopoverContent>
                  </Popover>
                </div>

                <div className="space-y-2">
                  <Label>End Date</Label>
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button
                        variant="outline"
                        className="w-full justify-start text-left font-normal"
                      >
                        <Calendar className="mr-2 h-4 w-4" />
                        {endDate ? formatDate(endDate) : "Select date"}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0" align="start">
                      <CalendarComponent
                        mode="single"
                        selected={endDate || undefined}
                        onSelect={(date) => setEndDate(date || null)}
                        initialFocus
                      />
                    </PopoverContent>
                  </Popover>
                </div>
              </div>

              {/* Endpoint Input */}
              <div className="space-y-2">
                <Label htmlFor="endpoint">Endpoint</Label>
                <Input
                  id="endpoint"
                  value={endpoint}
                  onChange={(e) => setEndpoint(e.target.value)}
                  placeholder="Enter endpoint path (e.g., /api/v1/search)"
                  disabled={isOlsSelected && hasOntologyId}
                />
              </div>

              {/* Parameters Section */}
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <Label>Parameters</Label>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={addParameter}
                    disabled={isOlsSelected && hasOntologyId}
                  >
                    Add Parameter
                  </Button>
                </div>
                
                {parameters.map((param, index) => (
                  <div key={index} className="flex gap-4">
                    <Input
                      placeholder="Key"
                      value={param.key}
                      onChange={(e) => updateParameter(index, 'key', e.target.value)}
                      className="flex-1"
                      disabled={isOlsSelected && hasOntologyId}
                    />
                    <Input
                      placeholder="Value"
                      value={param.value}
                      onChange={(e) => updateParameter(index, 'value', e.target.value)}
                      className="flex-1"
                      disabled={isOlsSelected && hasOntologyId}
                    />
                    {parameters.length > 1 && (
                      <Button
                        type="button"
                        variant="destructive"
                        size="icon"
                        onClick={() => removeParameter(index)}
                        disabled={isOlsSelected && hasOntologyId}
                      >
                        ×
                      </Button>
                    )}
                  </div>
                ))}
              </div>

              {/* Submit Button */}
              <Button 
                className="w-full"
                onClick={handleSearch}
                disabled={isSearching}
              >
                {isSearching ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Searching...
                  </>
                ) : (
                  'Search'
                )}
              </Button>

              {/* Error Display */}
              {error && (
                <Alert variant="destructive">
                  <AlertTitle>Error</AlertTitle>
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>

          {/* Simplified Results Display */}
          {searchResults && (
            <Card>
              <CardHeader>
                <CardTitle>Search Results</CardTitle>
                <CardDescription>
                  Statistics for {searchResults.resource === 'ALL' ? 'all resources' : searchResults.resource}
                  {selectedCountry !== 'ALL' && ` in ${selectedCountry}`}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-1">
                  <Label className="text-sm text-gray-500">Total Requests</Label>
                  <p className="text-3xl font-bold">{searchResults.matching_requests.toLocaleString()}</p>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
};

export default SpotStatsApp;