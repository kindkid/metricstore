desc "Generate and open coverage report"
task :coverage do
  system 'rake spec'
  system 'open coverage/index.html'
end

desc "Generate and open coverage report"
task :rcov do
  system 'rake coverage'
end