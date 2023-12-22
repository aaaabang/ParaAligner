using MPI
# using JSON

MPI.Init()
global batch_t = 1
const comm = MPI.Comm_dup(MPI.COMM_WORLD)  
const node_num = MPI.Comm_size(comm)
const rank = MPI.Comm_rank(comm)

mutable struct Tracing
    chainC::AbstractArray
    chainR::AbstractArray
    posR::Int
    posC::Int
    highest_score::Int
    is_continue::Bool
end

function split_string(s::AbstractString, N::Int)::AbstractArray # N is node_num
    len = length(s)
    if len % N != 0
        padding_length = (div(len,N) + 1) * N - len
        s *= repeat("M", padding_length)
        len += padding_length
    end
    result = Vector{String}(undef, N)
    step = div(len, N)
    start = 1
    for i in 1:N
        result[i] = s[start: start + step - 1]
        start += step
    end
    return result
end

function get_topK(matrix::AbstractMatrix, k::Int)::AbstractArray
    indices = sortperm(matrix[:], rev=true)[1:k]
    topK = Array{Int, 2}(undef, (4, k))
    for i in 1:k
        lenR = size(matrix)[1]
        indexR = indices[i] % lenR
        if indexR == 0
            indexR += lenR
        end
        indexC = div(indices[i],lenR)
        if indices[i] % lenR != 0
            indexC += 1
        end
        topK[1, i] = indexR + 1 # we abandon the first row of matrix
        topK[2, i] = indexC
        topK[3, i] = deepcopy(matrix[indexR, indexC])
        topK[4, i] = rank
    end
    return topK
end

function show_topK_all(topK_all)::Nothing
    if rank == 0
        println("topK all:")
        for row in eachrow(topK_all)
            println("$row")
        end
    end
    MPI.Barrier(comm)
    return nothing
end

function show(matrix)::Nothing
    for i in 1:node_num
        if rank == i-1
            println("rank $rank")
            for row in eachrow(matrix)
                println("$row")
            end
        end
        MPI.Barrier(comm)
    end
    MPI.Barrier(comm)
    return nothing
end

function show_traceback(trace::Tracing)::Nothing
    println("chainC(S) = $(join(reverse(trace.chainC)))")
    println("chainR(D) = $(join(reverse(trace.chainR)))")
    return nothing
end

function SmithWaterman_Par(chainS::AbstractString, chainD::AbstractString, rule::AbstractArray, k::Int)::Nothing # ::AbstractArray
    # get lenC and lenR
    lenC = length(chainD)
    if length(chainS) % node_num != 0
        lenR = div((div(length(chainS), node_num) + 1) * node_num, node_num)# (div(len,N) + 1) * N
    else
        lenR = div(length(chainS), node_num)
    end

    # prepare for data dependency
    if rank == 0 # root
        rule0 = rule
        chainR_send = split_string(chainS, node_num)
        chainC = collect(chainD)
        chainR = Vector{Char}(undef, lenR)
        topK_all = Array{Int, 2}(undef, (4, k * node_num))
        result_chains = Vector{Tracing}(undef,k)
    else # other workers
        chainR_send = Vector{Char}(undef, 0)
        rule0 = Vector{Int}(undef, 3)
        chainR = Vector{Char}(undef, lenR)
        chainC = Vector{Char}(undef, lenC)
        topK_all = Array{Int, 2}(undef, (4, k * node_num))
        result_chains = Vector{Tracing}(undef,0)
    end
    topK = Array{Int, 2}(undef, (4, k))

    # spread data dependency
    MPI.Bcast!(chainC, comm; root=0)
    MPI.Bcast!(rule0, comm; root=0)
    chainR = MPI.scatter(chainR_send, comm; root=0)
    chainC = vcat(['!'], chainC)
    chainR = vcat(['?'], collect(chainR))
    lenR += 1
    lenC += 1
    matrix = zeros(Int, (lenR,lenC))
    MPI.Barrier(comm)

    # fill the matrix
    for c in 2:lenC
        upper_cell = -1 # left
        lower_cell = -1 # right
        if rank != 0
            upper_cell = MPI.recv(comm; source=rank-1, tag=c)
            matrix[1, c] = deepcopy(upper_cell)
        end

        for r in 2:lenR
            if chainR[r] == 'M'
                matrix[r,c] = -1
            else
                if chainR[r] == chainC[c]
                    matrix[r,c] = max(matrix[r-1,c] + rule0[3],
                                    matrix[r,c-1] + rule0[3],
                                    matrix[r-1,c-1] + rule0[1],
                                    0)
                else
                    matrix[r,c] = max(matrix[r-1,c] + rule0[3], 
                                    matrix[r,c-1] + rule0[3], 
                                    matrix[r-1,c-1] + rule0[2],
                                    0)
                end
            end

            if r == lenR && rank != node_num - 1
                lower_cell = deepcopy(matrix[r, c])
                MPI.isend(lower_cell, comm; dest=rank+1, tag=c)
            end
        end
    end

    # get and gather topKs
    topK = get_topK(matrix[2:end, :], k) # row1: indexR, row2:indexC, row3:value, row4: rank
    MPI.Gather!(topK, topK_all, comm; root=0)
    if rank == 0
        sorted_index = sortperm(topK_all[3, :], rev=true)
        topK_all = topK_all[:, sorted_index][:, 1:k]
    end
    MPI.Bcast!(topK_all, comm; root=0)
    
    # show matrix, top k all
    # show(matrix)
    # show(topK)
    # show_topK_all(topK_all)

    # trace back
    for t in 1:k # 1:k, for top k
        if rank == 0
            println(topK_all[1,1])
            println(topK_all[2,1])
            println(topK_all[3,1])
            println(matrix[topK_all[1,1], topK_all[2,1]])
        end
        node_start = topK_all[4, t]
        if rank == node_start
            trace = Tracing(Vector{Char}(undef,0), Vector{Char}(undef,0), topK_all[1,t], topK_all[2,t], topK_all[3,t], true)
        elseif rank < node_start
            trace = MPI.recv(comm; source=rank+1, tag=t)
            trace.posR = lenR
        end

        if rank <= node_start
            while trace.posR >= 2 && trace.posC >= 2 && trace.is_continue == true
                if matrix[trace.posR, trace.posC] <= 0
                    trace.is_continue = false
                    continue
                end

                if matrix[trace.posR, trace.posC] >= matrix[trace.posR-1, trace.posC] && matrix[trace.posR, trace.posC] >= matrix[trace.posR, trace.posC-1] 
                    push!(trace.chainC, chainC[trace.posC])
                    push!(trace.chainR, chainR[trace.posR])
                    trace.posC -= 1
                    trace.posR -= 1
                elseif matrix[trace.posR-1, trace.posC] >= matrix[trace.posR, trace.posC-1]# go up
                    push!(trace.chainC, '-')
                    push!(trace.chainR, chainR[trace.posR])
                    trace.posR -= 1
                else # go left
                    push!(trace.chainC, chainC[trace.posC])
                    push!(trace.chainR, '-')
                    trace.posC -= 1
                end
            end
        end

        # after the while loop end, check the situation to decide send, or not continue
        if rank == 0 # append the result at rank0
            result_chains[t] = trace
        elseif rank <= node_start # send at other ranks
            MPI.send(trace,comm;dest=rank-1,tag=t)
        end
        # MPI.Barrier(comm) # pay attention to this Barrier
    end

    # return the result
    if rank == 0
        for t in 1:k
            writeTXT(result_chains[t], t)            
        end
    end

    return nothing
end

function openFNA(file_path::AbstractString)::AbstractString
    file = open(file_path, "r")
    chain_lines = split(read(file, String), '\n')
    chain = join(chain_lines[2:end],'\n')
    chain = replace(chain, '\r' => "")
    chain = replace(chain, '\r' => "")
    chain = uppercase(chain)
    close(file)
    return chain
end

function writeTXT(trace::Tracing, k::Int)::Nothing
    file_name = "result/S$(batch_t)_N$(node_num)_top_$(k)_alignment.txt"
    contentS = "Score of chain:" * "$(trace.highest_score)"
    contentR = "chianD: " * join(reverse(trace.chainR))
    contentC = "chainS: " * join(reverse(trace.chainC))
    open(file_name, "w") do file
        write(file, contentS * "\n" * contentR * "\n" * contentC)
    end
    return nothing
end

function Solution()::Nothing
    global batch_t
    rule = [3, -3, -2] # match(+int), mismatch(-int), gap(-int)
    k = 3 # top k
    chainD = openFNA("input/D.fna")
    while(true)
        try
            chainS = openFNA("input/covid$(batch_t).fna")
            time = @elapsed SmithWaterman_Par(chainS, chainD, rule, k)
            if rank == 0
                println("Mean time of task No.$(batch_t) is $(time) second(s).")
                file_name = "result/N$(node_num)_execution_time.txt"
                if batch_t == 1
                    open(file_name, "w") do file # Write a txt for exec time
                        write(file, "Task:$(batch_t)\nTime:$(time)\n---------------\n")
                    end
                else
                    open(file_name, "a") do file # Append the content
                        write(file, "Task:$(batch_t)\nTime:$(time)\n---------------\n")
                    end
                end
            end
            batch_t += 1
            # MPI.Barrier(comm) # pay attention to this Barrier
        catch
            if rank == 0
                println("Last job is #$(batch_t - 1).")
                println("All ended.")
            end
            break
        end
    end
    MPI.Barrier(comm)
    MPI.Finalize()
    return nothing
end

function Single_test()::Nothing
    global batch_t
    rule = [3, -3, -2] # match(+int), mismatch(-int), gap(-int)
    k = 1 # top k
    chainD = openFNA("input/test1.fna")
    chainS = openFNA("input/test.fna")
    if rank == 0
        println(chainD)
        println(chainS)
    end
    MPI.Barrier(comm)
    SmithWaterman_Par(chainS, chainD, rule, k)
    MPI.Finalize()
    return nothing
end

Single_test()
# Solution() # mpiexec -n 4 julia ./SmithWaterman_Par.jl